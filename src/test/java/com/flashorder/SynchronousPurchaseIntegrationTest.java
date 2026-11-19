package com.flashorder;

import com.flashorder.inventory.InventoryMapper;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.mysql.MySQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "flashorder.intake-mode=sync")
@Import(SynchronousPurchaseIntegrationTest.FaultConfiguration.class)
class SynchronousPurchaseIntegrationTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(45);
    private static final long USER = 101;
    private static final long OTHER_USER = 102;

    private final String keyPrefix = UUID.randomUUID() + ":";
    private final HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private ReservationFault reservationFault;

    @AfterEach
    void closeClient() {
        client.close();
    }

    @DynamicPropertySource
    static void configureDatabase(DynamicPropertyRegistry registry) {
        String externalUrl = System.getenv("FLASHORDER_TEST_JDBC_URL");
        if (externalUrl != null && !externalUrl.isBlank()) {
            registry.add("spring.datasource.url", () -> externalUrl);
            registry.add("spring.datasource.username",
                    () -> System.getenv().getOrDefault("FLASHORDER_TEST_DB_USER", "flashorder"));
            registry.add("spring.datasource.password",
                    () -> System.getenv().getOrDefault("FLASHORDER_TEST_DB_PASSWORD", "flashorder"));
            return;
        }

        MySQLContainer mysql = LocalMysql.INSTANCE;
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
    }

    @Test
    void databaseSessionUsesReadCommitted() {
        assertThat(jdbc.queryForObject("SELECT @@session.transaction_isolation", String.class))
                .isEqualTo("READ-COMMITTED");
    }

    @Test
    void twoHundredDifferentUsersCreateExactlyFiftyOrders() throws Exception {
        long campaign = activeCampaign(50);
        long eventCount = orderCreatedEvents();

        List<Reply> replies = submitConcurrently(200,
                index -> purchase(campaign, 1_000L + index, "stock-race"));

        assertThat(replies).allSatisfy(reply -> {
            if (reply.httpStatus() == 201) {
                assertCreated(reply);
            } else {
                assertRejected(reply, "SOLD_OUT");
            }
            assertThat(reply.replayHeader()).isNull();
        });
        assertThat(replies).filteredOn(reply -> reply.httpStatus() == 201).hasSize(50);
        assertThat(replies).extracting(reply -> reply.field("requestUid")).doesNotHaveDuplicates();
        assertThat(count("SELECT COUNT(*) FROM purchase_order WHERE campaign_id = ? AND status = 'PENDING'",
                campaign)).isEqualTo(50);
        assertThat(count("SELECT COUNT(*) FROM purchase_request WHERE campaign_id = ? AND status = 'ORDER_CREATED'",
                campaign)).isEqualTo(50);
        assertThat(count("""
                SELECT COUNT(*) FROM purchase_request
                WHERE campaign_id = ? AND status = 'REJECTED' AND reject_reason = 'SOLD_OUT' AND order_id IS NULL
                """, campaign)).isEqualTo(150);
        assertCampaign(campaign, 0, 200, 50);
        assertThat(orderCreatedEvents()).isEqualTo(eventCount + 50);
    }

    @Test
    void oneUserWithDifferentKeysCanOnlyReserveOnce() throws Exception {
        long campaign = activeCampaign(2);
        long eventCount = orderCreatedEvents();

        List<Reply> replies = submitConcurrently(2, index -> purchase(campaign, USER, "key-" + index));

        assertThat(replies).filteredOn(reply -> reply.httpStatus() == 201)
                .singleElement().satisfies(SynchronousPurchaseIntegrationTest::assertCreated);
        assertThat(replies).filteredOn(reply -> reply.httpStatus() == 409)
                .singleElement().satisfies(reply -> assertRejected(reply, "ALREADY_HOLDING"));
        assertThat(replies).extracting(reply -> reply.field("requestUid")).doesNotHaveDuplicates();
        assertThat(replies).extracting(Reply::replayHeader).containsOnlyNulls();
        assertCampaign(campaign, 1, 2, 1);
        assertThat(count("""
                SELECT COUNT(*) FROM purchase_request
                WHERE campaign_id = ? AND status = 'REJECTED' AND reject_reason = 'ALREADY_HOLDING'
                  AND order_id IS NULL
                """, campaign)).isEqualTo(1);
        assertThat(orderCreatedEvents()).isEqualTo(eventCount + 1);
    }

    @Test
    void fortyIdenticalPurchasesShareOneCreatedResult() throws Exception {
        long campaign = activeCampaign(1);
        long eventCount = orderCreatedEvents();

        List<Reply> replies = submitConcurrently(40, index -> purchase(campaign, USER, "same-key-in-stock"));

        assertThat(replies).allSatisfy(SynchronousPurchaseIntegrationTest::assertCreated);
        assertSameResult(replies);
        assertCampaign(campaign, 0, 1, 1);
        assertThat(orderCreatedEvents()).isEqualTo(eventCount + 1);
    }

    @Test
    void fortyIdenticalSoldOutPurchasesShareOnePersistedRejection() throws Exception {
        long campaign = activeCampaign(0);
        long eventCount = orderCreatedEvents();

        List<Reply> replies = submitConcurrently(40, index -> purchase(campaign, USER, "same-key-sold-out"));

        assertThat(replies).allSatisfy(reply -> assertRejected(reply, "SOLD_OUT"));
        assertSameResult(replies);
        assertCampaign(campaign, 0, 1, 0);
        assertThat(count("""
                SELECT COUNT(*) FROM purchase_request
                WHERE campaign_id = ? AND status = 'REJECTED' AND reject_reason = 'SOLD_OUT' AND order_id IS NULL
                """, campaign)).isEqualTo(1);
        assertThat(orderCreatedEvents()).isEqualTo(eventCount);
    }

    @Test
    void exceptionAfterStockUpdateRollsBackAndSameKeyCanSucceed() throws Exception {
        long campaign = activeCampaign(1);
        long eventCount = orderCreatedEvents();
        String key = "retry-after-rollback";

        reservationFault.arm(campaign);
        Reply failed;
        try {
            failed = send(purchase(campaign, USER, key));
            assertThat(reservationFault.isArmed()).isFalse();
        } finally {
            reservationFault.disarm();
        }

        assertThat(failed.httpStatus()).as(failed.body()).isBetween(500, 599);
        assertCampaign(campaign, 1, 0, 0);
        assertThat(orderCreatedEvents()).isEqualTo(eventCount);

        Reply retry = send(purchase(campaign, USER, key));

        assertCreated(retry);
        assertThat(retry.replayHeader()).isNull();
        assertCampaign(campaign, 0, 1, 1);
        assertThat(orderCreatedEvents()).isEqualTo(eventCount + 1);
    }

    @Test
    void sameUserCannotReuseAKeyForAnotherCampaign() throws Exception {
        long original = activeCampaign(1);
        long other = activeCampaign(1);
        String key = "scoped-to-user";

        assertCreated(send(purchase(original, USER, key)));
        Reply conflict = send(purchase(other, USER, key));

        assertEntryRejection(conflict, "IDEMPOTENCY_CONFLICT");
        assertCampaign(other, 1, 0, 0);

        Reply independent = send(purchase(other, OTHER_USER, key));
        assertCreated(independent);
        assertThat(independent.replayHeader()).isNull();
        assertCampaign(other, 0, 1, 1);
    }

    @Test
    void requestAndOrderAreVisibleOnlyToTheOwner() throws Exception {
        Reply created = send(purchase(activeCampaign(1), USER, "ownership"));
        assertCreated(created);

        String requestPath = "/purchase-requests/" + created.field("requestUid");
        String orderPath = "/orders/" + created.field("orderUid");
        Reply request = send(query(requestPath, USER));
        Reply order = send(query(orderPath, USER));

        assertThat(request.httpStatus()).isEqualTo(200);
        assertThat(request.field("status")).isEqualTo("ORDER_CREATED");
        assertThat(request.field("orderUid")).isEqualTo(created.field("orderUid"));
        assertThat(order.httpStatus()).isEqualTo(200);
        assertThat(order.field("status")).isEqualTo("PENDING");
        assertThat(send(query(requestPath, OTHER_USER)).httpStatus()).isEqualTo(404);
        assertThat(send(query(orderPath, OTHER_USER)).httpStatus()).isEqualTo(404);
    }

    @Test
    void campaignWindowRejectsNewRequestsWithoutDiscardingPreviousResults() throws Exception {
        long future = createCampaign(1, 1, 2);
        long active = activeCampaign(2);
        String originalKey = "accepted-before-close";
        Reply created = send(purchase(active, USER, originalKey));
        assertCreated(created);

        jdbc.update("UPDATE campaign SET ends_at = NOW(6) - INTERVAL 1 MINUTE WHERE id = ?", active);

        assertEntryRejection(send(purchase(future, USER, "too-early")), "CAMPAIGN_CLOSED");
        assertEntryRejection(send(purchase(active, OTHER_USER, "too-late")), "CAMPAIGN_CLOSED");
        Reply replay = send(purchase(active, USER, originalKey));

        assertCreated(replay);
        assertThat(replay.replayHeader()).isEqualTo("true");
        assertThat(replay.field("requestUid")).isEqualTo(created.field("requestUid"));
        assertThat(replay.field("orderUid")).isEqualTo(created.field("orderUid"));
        assertCampaign(future, 1, 0, 0);
        assertCampaign(active, 1, 1, 1);
    }

    private long activeCampaign(int stock) {
        return createCampaign(stock, -1, 1);
    }

    private long createCampaign(int stock, int startOffsetHours, int endOffsetHours) {
        String sku = UUID.randomUUID().toString();
        jdbc.update("""
                INSERT INTO campaign (sku, starts_at, ends_at)
                VALUES (?, TIMESTAMPADD(HOUR, ?, NOW(6)), TIMESTAMPADD(HOUR, ?, NOW(6)))
                """, sku, startOffsetHours, endOffsetHours);
        long id = jdbc.queryForObject("SELECT id FROM campaign WHERE sku = ?", Long.class, sku);
        jdbc.update("INSERT INTO inventory (campaign_id, total, available) VALUES (?, ?, ?)", id, stock, stock);
        return id;
    }

    private void assertCampaign(long campaign, int available, int requests, int orders) {
        assertThat(count("SELECT available FROM inventory WHERE campaign_id = ?", campaign)).isEqualTo(available);
        assertThat(count("SELECT COUNT(*) FROM purchase_request WHERE campaign_id = ?", campaign)).isEqualTo(requests);
        assertThat(count("SELECT COUNT(*) FROM purchase_order WHERE campaign_id = ?", campaign)).isEqualTo(orders);
        assertThat(count("SELECT total FROM inventory WHERE campaign_id = ?", campaign)).isEqualTo(available + orders);
    }

    private long orderCreatedEvents() {
        return count("SELECT COUNT(*) FROM outbox_message WHERE event_type = 'ORDER_CREATED'");
    }

    private long count(String sql, Object... parameters) {
        return jdbc.queryForObject(sql, Long.class, parameters);
    }

    private HttpRequest purchase(long campaign, long user, String key) {
        return request("/campaigns/" + campaign + "/purchase-requests", user)
                .header("Idempotency-Key", keyPrefix + key)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
    }

    private HttpRequest query(String path, long user) {
        return request(path, user).GET().build();
    }

    private HttpRequest.Builder request(String path, long user) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(REQUEST_TIMEOUT)
                .header("Accept", "application/json")
                .header("X-User-Id", Long.toString(user));
    }

    private Reply send(HttpRequest request) throws Exception {
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return new Reply(response.statusCode(), response.headers().firstValue("Idempotent-Replayed").orElse(null),
                response.body());
    }

    private List<Reply> submitConcurrently(int count, IntFunction<HttpRequest> requests) throws Exception {
        var ready = new CountDownLatch(count);
        var start = new CountDownLatch(1);
        var pending = new ArrayList<Future<Reply>>(count);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                for (int index = 0; index < count; index++) {
                    HttpRequest request = requests.apply(index);
                    pending.add(executor.submit(() -> {
                        ready.countDown();
                        if (!start.await(15, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Concurrent purchase start timed out");
                        }
                        return send(request);
                    }));
                }
                assertThat(ready.await(15, TimeUnit.SECONDS)).isTrue();
            } finally {
                start.countDown();
            }

            var replies = new ArrayList<Reply>(count);
            for (Future<Reply> future : pending) {
                replies.add(future.get(60, TimeUnit.SECONDS));
            }
            return replies;
        }
    }

    private static void assertSameResult(List<Reply> replies) {
        Reply first = replies.getFirst();
        assertThat(replies).allSatisfy(reply -> {
            assertThat(reply.httpStatus()).isEqualTo(first.httpStatus());
            assertThat(reply.field("requestUid")).isEqualTo(first.field("requestUid"));
            assertThat(reply.field("orderUid")).isEqualTo(first.field("orderUid"));
            assertThat(reply.field("reason")).isEqualTo(first.field("reason"));
        });
        assertThat(replies).filteredOn(reply -> "true".equals(reply.replayHeader())).hasSize(39);
        assertThat(replies).filteredOn(reply -> reply.replayHeader() == null).hasSize(1);
    }

    private static void assertCreated(Reply reply) {
        assertThat(reply.httpStatus()).as(reply.body()).isEqualTo(201);
        assertThat(reply.field("status")).isEqualTo("ORDER_CREATED");
        assertThat(reply.field("reason")).isNull();
        assertUuidV7(reply.field("requestUid"));
        assertUuidV7(reply.field("orderUid"));
    }

    private static void assertRejected(Reply reply, String reason) {
        assertThat(reply.httpStatus()).as(reply.body()).isEqualTo(409);
        assertThat(reply.field("status")).isEqualTo("REJECTED");
        assertThat(reply.field("reason")).isEqualTo(reason);
        assertThat(reply.field("orderUid")).isNull();
        assertUuidV7(reply.field("requestUid"));
    }

    private static void assertEntryRejection(Reply reply, String reason) {
        assertThat(reply.httpStatus()).as(reply.body()).isEqualTo(409);
        assertThat(reply.field("reason")).isEqualTo(reason);
        assertThat(reply.field("requestUid")).isNull();
        assertThat(reply.field("orderUid")).isNull();
        assertThat(reply.replayHeader()).isNull();
    }

    private static void assertUuidV7(String text) {
        assertThat(text).isNotBlank();
        assertThat(UUID.fromString(text).version()).isEqualTo(7);
    }

    private record Reply(int httpStatus, String replayHeader, String body) {
        String field(String name) {
            if (body.isBlank()) {
                return null;
            }
            JsonNode value = JSON.readTree(body).get(name);
            return value == null || value.isNull() ? null : value.asString();
        }
    }

    private static final class LocalMysql {
        private static final MySQLContainer INSTANCE = startMysql();

        private static MySQLContainer startMysql() {
            var mysql = new MySQLContainer("mysql:8.4")
                    .withDatabaseName("flashorder_test")
                    .withUsername("flashorder")
                    .withPassword("flashorder")
                    .withCommand("--transaction-isolation=READ-COMMITTED", "--default-time-zone=+00:00");
            mysql.start();
            return mysql;
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class FaultConfiguration {
        @Bean
        static ReservationFault reservationFault() {
            return new ReservationFault();
        }
    }

    static final class ReservationFault implements BeanPostProcessor {
        private final AtomicLong armedCampaign = new AtomicLong();

        void arm(long campaign) {
            armedCampaign.set(campaign);
        }

        boolean isArmed() {
            return armedCampaign.get() != 0;
        }

        void disarm() {
            armedCampaign.set(0);
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (!(bean instanceof InventoryMapper mapper)) {
                return bean;
            }

            var proxy = new ProxyFactory(mapper);
            proxy.addAdvice((MethodInterceptor) invocation -> {
                Object result = invocation.proceed();
                if (invocation.getMethod().getName().equals("reserve") && Integer.valueOf(1).equals(result)
                        && armedCampaign.compareAndSet((Long) invocation.getArguments()[0], 0)) {
                    throw new IllegalStateException("Injected failure after successful stock update");
                }
                return result;
            });
            return proxy.getProxy();
        }
    }
}
