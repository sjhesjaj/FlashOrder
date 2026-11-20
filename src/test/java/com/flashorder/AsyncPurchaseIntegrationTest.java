package com.flashorder;

import com.flashorder.outbox.OutboxRelay;
import com.flashorder.outbox.OutboxRelayStore;
import com.flashorder.request.OrderCommandListener;
import com.flashorder.request.OrderCommandMapper;
import com.flashorder.request.PurchaseOrderProcessor;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ActiveProfiles({"api", "scheduler", "consumer"})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "flashorder.intake-mode=async", "flashorder.outbox-relay.scheduling-enabled=false"
})
class AsyncPurchaseIntegrationTest {
    private static final String COMMANDS = "flashorder.order-commands";
    private static final String DLT = "flashorder.order-commands.DLT";
    private static final String GROUP = "flashorder-order-workers";
    private static final long USER = 71;
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    private final HttpClient client = HttpClient.newHttpClient();

    @LocalServerPort
    private int port;
    @Value("${spring.kafka.bootstrap-servers}")
    private String brokers;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private KafkaTemplate<String, String> kafka;
    @Autowired
    private OutboxRelay relay;
    @Autowired
    private SqlSessionTemplate sqlSession;
    @MockitoSpyBean
    private OutboxRelayStore relayStore;
    @MockitoSpyBean
    private OrderCommandListener listener;
    @MockitoSpyBean
    private PurchaseOrderProcessor orders;
    @MockitoSpyBean
    private OrderCommandMapper commands;

    @DynamicPropertySource
    static void configureInfrastructure(DynamicPropertyRegistry properties) {
        var mysql = MySqlTestDatabase.startMysql("flashorder_async_test");
        var broker = new KafkaContainer("apache/kafka:4.2.2");
        broker.start();
        properties.add("spring.datasource.url", mysql::getJdbcUrl);
        properties.add("spring.datasource.username", mysql::getUsername);
        properties.add("spring.datasource.password", mysql::getPassword);
        properties.add("spring.kafka.bootstrap-servers", broker::getBootstrapServers);
    }

    @AfterEach
    void closeClient() {
        client.close();
    }

    @Test
    void lostHttpResponseReplaysDurableAdmissionAndClosedCampaignStillFinishes() throws Exception {
        long campaign = campaign(1);
        String key = UUID.randomUUID().toString();
        try (Socket socket = new Socket("127.0.0.1", port)) {
            String request = "POST /campaigns/" + campaign + "/purchase-requests HTTP/1.1\r\n"
                    + "Host: 127.0.0.1:" + port + "\r\nContent-Length: 0\r\n"
                    + "X-User-Id: " + USER + "\r\nIdempotency-Key: " + key + "\r\n\r\n";
            socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            // The client closes the connection after MySQL commits without reading the response.
            await(() -> count("SELECT COUNT(*) FROM purchase_request WHERE user_id = ? AND idempotency_key = ?",
                    USER, key) == 1);
        }

        Reply replay = submit(campaign, key);
        String uid = acceptedUid(replay);
        assertThat(replay.replayed()).isEqualTo("true");
        assertThat(replay.location()).isEqualTo("/purchase-requests/" + uid);
        assertThat(replay.retryAfter()).isEqualTo("1");
        Reply pending = get(uid, USER);
        assertThat(pending.status()).isEqualTo(200);
        assertThat(pending.body().get("status").asString()).isEqualTo("ACCEPTED");
        assertThat(pending.retryAfter()).isEqualTo("1");
        assertThat(get(uid, USER + 1).status()).isEqualTo(404);
        assertThat(events(uid, "CREATE_ORDER")).isEqualTo(1);
        assertThat(stock(campaign)).isEqualTo(1);

        jdbc.update("UPDATE campaign SET ends_at = NOW(6) - INTERVAL 1 MINUTE WHERE id = ?", campaign);
        relay.publishBatch();
        await(() -> status(uid).equals("ORDER_CREATED"));
        Reply created = submit(campaign, key);
        assertThat(created.status()).isEqualTo(201);
        assertThat(created.replayed()).isEqualTo("true");
        assertThat(created.body().get("requestUid").asString()).isEqualTo(uid);
        assertOrderOnce(uid, campaign);
    }

    @Test
    void concurrentSameKeyAdmissionWritesOneCommandAndBusinessRejectionIsTerminal() throws Exception {
        long campaign = campaign(0);
        String key = UUID.randomUUID().toString();
        var replies = new ArrayList<Reply>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            CountDownLatch start = new CountDownLatch(1);
            var futures = IntStream.range(0, 40).mapToObj(index -> executor.submit(() -> {
                if (!start.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Admission race did not start");
                }
                return submit(campaign, key);
            })).toList();
            start.countDown();
            for (var future : futures) {
                replies.add(future.get(20, TimeUnit.SECONDS));
            }
        }
        String uid = acceptedUid(replies.getFirst());
        assertThat(replies).allSatisfy(reply -> assertThat(acceptedUid(reply)).isEqualTo(uid));
        assertThat(replies).filteredOn(reply -> "true".equals(reply.replayed())).hasSize(39);
        assertThat(events(uid, "CREATE_ORDER")).isEqualTo(1);
        relay.publishBatch();
        await(() -> status(uid).equals("REJECTED"));
        Reply rejected = submit(campaign, key);
        assertThat(rejected.status()).isEqualTo(409);
        assertThat(rejected.body().get("reason").asString()).isEqualTo("SOLD_OUT");
        assertThat(rejected.replayed()).isEqualTo("true");
        assertThat(orderCount(uid)).isZero();
        assertThat(events(uid, "ORDER_CREATED")).isZero();
    }

    @Test
    void kafkaAckBeforeRelayCrashRedeliversExpiredLeaseWithoutDuplicateOrder() throws Exception {
        long campaign = campaign(1);
        String uid = acceptedUid(submit(campaign, UUID.randomUUID().toString()));
        long messageId = count("SELECT id FROM outbox_message WHERE event_type = 'CREATE_ORDER'"
                + " AND JSON_UNQUOTE(JSON_EXTRACT(payload, '$.requestUid')) = ?", uid);

        doAnswer(invocation -> {
            List<Long> ids = invocation.getArgument(1);
            if (ids.contains(messageId)) {
                throw new IllegalStateException("Injected crash after Kafka ACK");
            }
            return invocation.callRealMethod();
        }).when(relayStore).markPublished(anyString(), any());

        assertThatThrownBy(relay::publishBatch).hasMessageContaining("Injected crash after Kafka ACK");
        await(() -> status(uid).equals("ORDER_CREATED"));
        var first = readMatching(COMMANDS, record -> record.value().contains(uid), 1);
        assertThat(first.getFirst().key()).isEqualTo(Long.toString(campaign));
        assertThat(count("SELECT COUNT(*) FROM outbox_message WHERE id = ? AND published_at IS NULL"
                + " AND lease_token IS NOT NULL", messageId)).isEqualTo(1);

        doAnswer(invocation -> invocation.callRealMethod()).when(relayStore).markPublished(anyString(), any());
        jdbc.update("UPDATE outbox_message SET lease_until = NOW(6) - INTERVAL 1 SECOND WHERE id = ?", messageId);
        relay.publishBatch();
        var duplicates = readMatching(COMMANDS, record -> record.value().contains(uid), 2);
        assertThat(duplicates.get(0).value()).isEqualTo(duplicates.get(1).value());
        awaitCommitted(duplicates.getLast());
        assertOrderOnce(uid, campaign);
        assertThat(count("SELECT COUNT(*) FROM outbox_message WHERE id = ? AND published_at IS NOT NULL"
                + " AND lease_token IS NULL", messageId)).isEqualTo(1);
    }

    @Test
    void staleRelayTokenCannotConfirmAnotherWorkersLease() throws Exception {
        long campaign = campaign(1);
        String uid = acceptedUid(submit(campaign, UUID.randomUUID().toString()));
        long messageId = count("SELECT id FROM outbox_message WHERE event_type = 'CREATE_ORDER'"
                + " AND JSON_UNQUOTE(JSON_EXTRACT(payload, '$.requestUid')) = ?", uid);
        var oldLease = relayStore.claimBatch(20, 30_000);
        assertThat(oldLease.messages()).anyMatch(message -> message.id() == messageId);
        jdbc.update("UPDATE outbox_message SET lease_until = NOW(6) - INTERVAL 1 SECOND WHERE id = ?", messageId);
        var newLease = relayStore.claimBatch(20, 30_000);
        assertThat(newLease.messages()).anyMatch(message -> message.id() == messageId);

        assertThat(relayStore.markPublished(oldLease.leaseToken(), List.of(messageId))).isZero();
        assertThat(relayStore.retrySoon(oldLease.leaseToken(), List.of(messageId), 1)).isZero();
        assertThat(count("SELECT COUNT(*) FROM outbox_message WHERE id = ? AND published_at IS NULL"
                + " AND lease_token = ? AND lease_until > NOW(6) + INTERVAL 10 SECOND",
                messageId, newLease.leaseToken())).isEqualTo(1);
        assertThat(relayStore.retrySoon(newLease.leaseToken(), List.of(messageId), 1)).isEqualTo(1);
        await(() -> count("SELECT COUNT(*) FROM outbox_message WHERE id = ? AND lease_until <= NOW(6)",
                messageId) == 1);
        relay.publishBatch();
        await(() -> status(uid).equals("ORDER_CREATED"));
        assertOrderOnce(uid, campaign);
    }

    @Test
    void listenerFailsAfterMysqlCommitBeforeOffsetAndReplaysCompletedRequest() throws Exception {
        long campaign = campaign(1);
        String uid = acceptedUid(submit(campaign, UUID.randomUUID().toString()));
        var committed = new CompletableFuture<ConsumerRecord<String, String>>();
        var release = new CountDownLatch(1);
        var deliveries = new AtomicInteger();
        doAnswer(invocation -> {
            ConsumerRecord<String, String> record = invocation.getArgument(0);
            invocation.callRealMethod();
            if (record.value().contains(uid) && deliveries.incrementAndGet() == 1) {
                committed.complete(record);
                if (!release.await(12, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Offset test did not release listener");
                }
                throw new IllegalStateException("Injected failure before offset commit");
            }
            return null;
        }).when(listener).onCommand(any());

        ConsumerRecord<String, String> record;
        try {
            relay.publishBatch();
            record = committed.get(12, TimeUnit.SECONDS);
            assertThat(status(uid)).isEqualTo("ORDER_CREATED");
            assertThat(committedOffset(record)).isLessThan(record.offset() + 1);
        } finally {
            release.countDown();
        }
        awaitCommitted(record);
        assertThat(deliveries.get()).isEqualTo(2);
        assertOrderOnce(uid, campaign);
    }

    @Test
    void retryExhaustionCommitsFailedStatusAndDurableDeadLetter() throws Exception {
        long campaign = campaign(1);
        String key = UUID.randomUUID().toString();
        String uid = acceptedUid(submit(campaign, key));
        doThrow(new IllegalStateException("Injected persistent order failure"))
                .when(orders).attemptOrder(anyLong(), eq(uid), eq(campaign), eq(USER));

        relay.publishBatch();
        await(() -> status(uid).equals("SYSTEM_FAILED"));
        verify(orders, times(4)).attemptOrder(anyLong(), eq(uid), eq(campaign), eq(USER));
        assertThat(count("SELECT COUNT(*) FROM outbox_message WHERE topic = ? AND CAST(payload AS CHAR) LIKE ?"
                + " AND published_at IS NULL", DLT, "%" + uid + "%")).isEqualTo(1);
        Reply failed = submit(campaign, key);
        assertThat(failed.status()).isEqualTo(409);
        assertThat(failed.replayed()).isEqualTo("true");
        assertThat(failed.body().get("reason").asString()).isEqualTo("REQUEST_FAILED");
        assertThat(get(uid, USER).body()).isEqualTo(failed.body());
        assertThat(orderCount(uid)).isZero();
        assertThat(stock(campaign)).isEqualTo(1);

        relay.publishBatch();
        readMatching(DLT, record -> record.value().contains(uid), 1);
    }

    @Test
    void failedDeadLetterInsertRollsBackFailureStatusAndDoesNotAdvanceOffset() throws Exception {
        String uid = acceptedUid(submit(campaign(1), UUID.randomUUID().toString()));
        doThrow(new IllegalStateException("Injected persistent order failure"))
                .when(orders).attemptOrder(anyLong(), eq(uid), anyLong(), eq(USER));
        var recoveries = new AtomicInteger();
        var secondRecovery = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        doAnswer(invocation -> {
            int attempt = recoveries.incrementAndGet();
            if (attempt == 1) {
                throw new IllegalStateException("Injected write failure during dead-letter recovery");
            }
            secondRecovery.countDown();
            if (!release.await(12, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Dead-letter test did not release recovery");
            }
            OrderCommandMapper.DeadLetter deadLetter = invocation.getArgument(0);
            return sqlSession.getMapper(OrderCommandMapper.class).insertDeadLetter(deadLetter);
        }).when(commands).insertDeadLetter(argThat(deadLetter ->
                deadLetter != null && uid.equals(deadLetter.requestUid())));

        ConsumerRecord<String, String> record;
        try {
            relay.publishBatch();
            record = readMatching(COMMANDS, delivery -> delivery.value().contains(uid), 1).getFirst();
            assertThat(secondRecovery.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(status(uid)).isEqualTo("ACCEPTED");
            assertThat(events(uid, "ORDER_COMMAND_FAILED")).isZero();
            assertThat(committedOffset(record)).isLessThan(record.offset() + 1);
        } finally {
            release.countDown();
        }
        awaitCommitted(record);
        assertThat(status(uid)).isEqualTo("SYSTEM_FAILED");
        assertThat(events(uid, "ORDER_COMMAND_FAILED")).isEqualTo(1);
        assertThat(recoveries.get()).isEqualTo(2);
    }

    @Test
    void malformedCommandReachesDeadLetterWithoutChangingAnUnrelatedRequest() throws Exception {
        long campaign = campaign(1);
        String uid = acceptedUid(submit(campaign, UUID.randomUUID().toString()));
        String invalid = "not-json-" + UUID.randomUUID();
        kafka.send(COMMANDS, Long.toString(campaign), invalid).get(10, TimeUnit.SECONDS);
        await(() -> count("SELECT COUNT(*) FROM outbox_message WHERE topic = ? AND CAST(payload AS CHAR) LIKE ?",
                DLT, "%" + invalid + "%") == 1);
        assertThat(status(uid)).isEqualTo("ACCEPTED");
        assertThat(orderCount(uid)).isZero();
        relay.publishBatch();
        await(() -> status(uid).equals("ORDER_CREATED"));
        readMatching(DLT, record -> record.value().contains(invalid), 1);
    }

    @Test
    void outboxRetentionOnlyRemovesOldPublishedRows() throws Exception {
        long campaign = campaign(1);
        String uid = acceptedUid(submit(campaign, UUID.randomUUID().toString()));
        relay.publishBatch();
        await(() -> status(uid).equals("ORDER_CREATED"));
        jdbc.update("""
                UPDATE outbox_message SET published_at = NOW(6) - INTERVAL 8 DAY
                WHERE event_type = 'CREATE_ORDER' AND JSON_UNQUOTE(JSON_EXTRACT(payload, '$.requestUid')) = ?
                """, uid);
        assertThat(relayStore.deletePublishedBatch(500)).isEqualTo(1);
        assertThat(events(uid, "CREATE_ORDER")).isZero();
        assertThat(events(uid, "ORDER_CREATED")).isEqualTo(1);
        assertThat(orderCount(uid)).isEqualTo(1);
    }

    private static String acceptedUid(Reply reply) {
        assertThat(reply.status()).as("%s", reply.body()).isEqualTo(202);
        assertThat(reply.body().get("status").asString()).isEqualTo("ACCEPTED");
        return reply.body().get("requestUid").asString();
    }

    private long campaign(int stock) {
        String sku = UUID.randomUUID().toString();
        jdbc.update("""
                INSERT INTO campaign (sku, starts_at, ends_at)
                VALUES (?, NOW(6) - INTERVAL 1 HOUR, NOW(6) + INTERVAL 1 HOUR)
                """, sku);
        long campaign = count("SELECT id FROM campaign WHERE sku = ?", sku);
        jdbc.update("INSERT INTO inventory (campaign_id, total, available) VALUES (?, ?, ?)", campaign, stock, stock);
        return campaign;
    }

    private long count(String sql, Object... arguments) {
        return jdbc.queryForObject(sql, Long.class, arguments);
    }

    private long stock(long campaign) {
        return count("SELECT available FROM inventory WHERE campaign_id = ?", campaign);
    }

    private String status(String uid) {
        return jdbc.queryForObject("SELECT status FROM purchase_request WHERE request_uid = ?", String.class, uid);
    }

    private long orderCount(String uid) {
        return count("SELECT COUNT(*) FROM purchase_order o JOIN purchase_request r ON o.request_id = r.id"
                + " WHERE r.request_uid = ?", uid);
    }

    private long events(String uid, String eventType) {
        return count("SELECT COUNT(*) FROM outbox_message WHERE event_type = ?"
                + " AND JSON_UNQUOTE(JSON_EXTRACT(payload, '$.requestUid')) = ?", eventType, uid);
    }

    private void assertOrderOnce(String uid, long campaign) {
        assertThat(orderCount(uid)).isEqualTo(1);
        assertThat(events(uid, "ORDER_CREATED")).isEqualTo(1);
        assertThat(stock(campaign)).isZero();
    }

    private Reply submit(long campaign, String key) throws Exception {
        return send(request("/campaigns/" + campaign + "/purchase-requests", USER)
                .header("Idempotency-Key", key).POST(HttpRequest.BodyPublishers.noBody()).build());
    }

    private Reply get(String uid, long user) throws Exception {
        return send(request("/purchase-requests/" + uid, user).GET().build());
    }

    private HttpRequest.Builder request(String path, long user) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(TIMEOUT).header("X-User-Id", Long.toString(user));
    }

    private Reply send(HttpRequest request) throws Exception {
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var headers = response.headers();
        return new Reply(response.statusCode(), headers.firstValue("Idempotent-Replayed").orElse(null),
                headers.firstValue("Location").orElse(null), headers.firstValue("Retry-After").orElse(null),
                response.body().isBlank() ? null : JSON.readTree(response.body()));
    }

    private long committedOffset(ConsumerRecord<String, String> record) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", brokers))) {
            return committedOffset(admin, record);
        }
    }

    private static long committedOffset(Admin admin, ConsumerRecord<String, String> record) throws Exception {
        var partition = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata offset = admin.listConsumerGroupOffsets(GROUP)
                .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS).get(partition);
        return offset == null ? -1 : offset.offset();
    }

    private void awaitCommitted(ConsumerRecord<String, String> record) throws Exception {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", brokers))) {
            while (committedOffset(admin, record) <= record.offset()) {
                assertThat(System.nanoTime()).as("Kafka offset was not committed").isLessThan(deadline);
                Thread.sleep(50);
            }
        }
    }

    private List<ConsumerRecord<String, String>> readMatching(
            String topic, Predicate<ConsumerRecord<String, String>> match, int expected) {
        var config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        var records = new ArrayList<ConsumerRecord<String, String>>();
        try (var reader = new KafkaConsumer<String, String>(config)) {
            var partitions = reader.partitionsFor(topic, TIMEOUT).stream()
                    .map(partition -> new TopicPartition(topic, partition.partition())).toList();
            reader.assign(partitions);
            reader.seekToBeginning(partitions);
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (records.size() < expected && System.nanoTime() < deadline) {
                reader.poll(Duration.ofMillis(250)).forEach(record -> {
                    if (match.test(record)) {
                        records.add(record);
                    }
                });
            }
        }
        assertThat(records).as("Matching records in %s", topic).hasSize(expected);
        return records;
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime()).as("Expected database state not reached").isLessThan(deadline);
            Thread.sleep(30);
        }
    }

    private record Reply(int status, String replayed, String location, String retryAfter, JsonNode body) {
    }
}
