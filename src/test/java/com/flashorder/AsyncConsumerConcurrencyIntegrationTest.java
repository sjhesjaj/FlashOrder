package com.flashorder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

import com.flashorder.request.PurchaseOrderProcessor;
import com.flashorder.request.PurchaseRequestService;
import com.flashorder.request.PurchaseResult;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.LongStream;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.kafka.KafkaContainer;

@ActiveProfiles("consumer")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "flashorder.intake-mode=async", "FLASHORDER_CONSUMER_CONCURRENCY=2"
})
class AsyncConsumerConcurrencyIntegrationTest {
    private static final String COMMANDS = "flashorder.order-commands";
    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final long FIRST_USER = 701;
    private static final long SECOND_USER = 702;

    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private KafkaTemplate<String, String> kafka;
    @Autowired
    private KafkaListenerEndpointRegistry listeners;
    @Autowired
    private PurchaseRequestService requests;
    @MockitoSpyBean
    private PurchaseOrderProcessor orders;

    @DynamicPropertySource
    static void configureInfrastructure(DynamicPropertyRegistry properties) {
        var mysql = MySqlTestDatabase.startMysql("flashorder_consumer_concurrency_test");
        var broker = new KafkaContainer("apache/kafka:4.2.2");
        broker.start();
        properties.add("spring.datasource.url", mysql::getJdbcUrl);
        properties.add("spring.datasource.username", mysql::getUsername);
        properties.add("spring.datasource.password", mysql::getPassword);
        properties.add("spring.kafka.bootstrap-servers", broker::getBootstrapServers);
    }

    @Test
    void distinctCampaignPartitionsProgressConcurrentlyWhileOneCampaignKeepsOrder() throws Exception {
        var listener = (ConcurrentMessageListenerContainer<?, ?>) listeners.getListenerContainer("flashorder-order-commands");
        assertThat(listener).isNotNull();
        assertThat(listener.getConcurrency()).isEqualTo(2);
        var workers = listener.getContainers();
        assertThat(workers).hasSize(2);
        await("Both Kafka workers receive partitions", () -> workers.stream().allMatch(worker -> {
            var assigned = worker.getAssignedPartitions();
            return assigned != null && !assigned.isEmpty();
        }));

        int firstPartition = workers.getFirst().getAssignedPartitions().iterator().next().partition();
        int otherPartition = workers.getLast().getAssignedPartitions().iterator().next().partition();
        assertThat(firstPartition).isNotEqualTo(otherPartition);
        int partitionCount = kafka.partitionsFor(COMMANDS).size();
        long firstCampaign = createCampaignForPartition(firstPartition, partitionCount);
        long otherCampaign = createCampaignForPartition(otherPartition, partitionCount);

        String firstUid = accept(firstCampaign, FIRST_USER);
        String secondUid = accept(firstCampaign, SECOND_USER);
        String otherUid = accept(otherCampaign, FIRST_USER);

        var firstStarted = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        doAnswer(invocation -> {
            firstStarted.countDown();
            if (!releaseFirst.await(TIMEOUT.multipliedBy(3).toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Timed out waiting to release the first order command");
            }
            return invocation.callRealMethod();
        }).when(orders).attemptOrder(anyLong(), eq(firstUid), eq(firstCampaign), eq(FIRST_USER));

        try {
            var firstRecord = publish(firstUid);
            assertThat(firstRecord.partition()).isEqualTo(firstPartition);
            assertThat(firstStarted.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();

            var secondRecord = publish(secondUid);
            var otherRecord = publish(otherUid);
            assertThat(secondRecord.partition()).isEqualTo(firstPartition);
            assertThat(secondRecord.offset()).isGreaterThan(firstRecord.offset());
            assertThat(otherRecord.partition()).isEqualTo(otherPartition);
            await("The other campaign commits while the first command is held",
                    () -> result(FIRST_USER, otherUid).status().equals("ORDER_CREATED"));
            assertThat(result(FIRST_USER, firstUid).status()).isEqualTo("ACCEPTED");
            assertThat(result(SECOND_USER, secondUid).status()).isEqualTo("ACCEPTED");
        } finally {
            releaseFirst.countDown();
        }

        await("The second command waits for the first command to take the only item",
                () -> result(SECOND_USER, secondUid).status().equals("REJECTED"));
        assertThat(result(FIRST_USER, firstUid).status()).isEqualTo("ORDER_CREATED");
        assertThat(result(SECOND_USER, secondUid).reason()).isEqualTo("SOLD_OUT");
    }

    private long createCampaignForPartition(int partition, int partitionCount) {
        long campaign = LongStream.rangeClosed(1, 1_000)
                .filter(id -> Utils.toPositive(Utils.murmur2(Long.toString(id).getBytes(StandardCharsets.UTF_8)))
                        % partitionCount == partition)
                .findFirst()
                .orElseThrow();
        jdbc.update("""
                INSERT INTO campaign (id, sku, starts_at, ends_at)
                VALUES (?, ?, NOW(6) - INTERVAL 1 HOUR, NOW(6) + INTERVAL 1 HOUR)
                """, campaign, "parallel-" + campaign);
        jdbc.update("INSERT INTO inventory (campaign_id, total, available) VALUES (?, 1, 1)", campaign);
        return campaign;
    }

    private String accept(long campaign, long user) {
        var result = requests.submit(campaign, user, UUID.randomUUID().toString()).result();
        assertThat(result.status()).isEqualTo("ACCEPTED");
        return result.requestUid();
    }

    private PurchaseResult result(long user, String uid) {
        return requests.find(user, uid).orElseThrow();
    }

    private RecordMetadata publish(String uid) throws Exception {
        var command = jdbc.queryForObject("""
                SELECT msg_key, payload FROM outbox_message
                WHERE event_type = 'CREATE_ORDER'
                  AND JSON_UNQUOTE(JSON_EXTRACT(payload, '$.requestUid')) = ?
                """, (row, rowNum) -> new StoredCommand(row.getString("msg_key"), row.getString("payload")), uid);
        return kafka.send(COMMANDS, command.key(), command.payload())
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getRecordMetadata();
    }

    private static void await(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime()).as(description).isLessThan(deadline);
            Thread.sleep(30);
        }
    }

    private record StoredCommand(String key, String payload) {
    }
}
