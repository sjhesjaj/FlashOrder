package com.flashorder;

import com.flashorder.order.OrderExpiryService;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("api")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "flashorder.order-expiry.batch-size=25",
                "spring.datasource.hikari.connection-init-sql=SET SESSION innodb_lock_wait_timeout=12"
        })
class OrderLifecycleIntegrationTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final long USER = 101;
    private static final long OTHER_USER = 102;

    private final String prefix = UUID.randomUUID() + ":";
    private final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

    @LocalServerPort
    private int port;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private DataSource dataSource;
    @Autowired
    private OrderExpiryService expiry;
    @Autowired
    private MeterRegistry meters;

    @DynamicPropertySource
    static void configureDatabase(DynamicPropertyRegistry registry) {
        MySqlTestDatabase.configure(registry);
    }

    @AfterEach
    void closeClient() {
        client.close();
    }

    @Test
    void successfulPaymentDoesNotWaitForInventoryAndReconciles() throws Exception {
        long campaign = campaign(1);
        String order = order(campaign, USER);
        Reply paid;
        try (Connection blocker = lockRow("SELECT available FROM inventory WHERE campaign_id = ? FOR UPDATE", campaign)) {
            paid = send(payment(order, USER, "pay"));
        }
        assertPaid(paid, order);
        assertThat(count("SELECT COUNT(*) FROM purchase_order WHERE order_uid = ? AND status = 'PAID'"
                + " AND paid_at < expire_at", order)).isEqualTo(1);
        assertThat(attempts(order, "SUCCEEDED")).isEqualTo(1);
        assertThat(events(order, "ORDER_PAID")).isEqualTo(1);
        assertReconciliation(campaign, 0, 0, 1);
    }

    @Test
    void fortyConcurrentPaymentsWithSameKeyReplayOneResult() throws Exception {
        String order = order(campaign(1), USER);
        double replaysBefore = paymentReplayCount();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var start = new CountDownLatch(1);
            var payments = race(executor, start, 40, i -> payment(order, USER, "same"));
            start.countDown();
            var replies = collect(payments);
            Reply first = replies.getFirst();
            assertThat(replies).allSatisfy(reply -> {
                assertPaid(reply, order);
                assertThat(reply.body()).isEqualTo(first.body());
            });
            assertThat(replies).filteredOn(reply -> "true".equals(reply.replay())).hasSize(39);
            assertThat(replies).filteredOn(reply -> reply.replay() == null).hasSize(1);
        }
        assertThat(attempts(order, "SUCCEEDED")).isEqualTo(1);
        assertThat(attempts(order, "ALREADY_PAID")).isZero();
        assertThat(events(order, "ORDER_PAID")).isEqualTo(1);
        assertThat(paymentReplayCount()).isEqualTo(replaysBefore + 39);
    }

    @Test
    void differentPaymentKeysRecordAlreadyPaidWithoutExtraEvents() throws Exception {
        String order = order(campaign(1), USER);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var start = new CountDownLatch(1);
            var payments = race(executor, start, 20, i -> payment(order, USER, "pay-" + i));
            start.countDown();
            var replies = collect(payments);
            assertThat(replies).allSatisfy(reply -> {
                assertPaid(reply, order);
                assertThat(reply.replay()).isNull();
                assertThat(reply.body()).isEqualTo(replies.getFirst().body());
            });
        }
        assertThat(attempts(order, "SUCCEEDED")).isEqualTo(1);
        assertThat(attempts(order, "ALREADY_PAID")).isEqualTo(19);
        assertThat(events(order, "ORDER_PAID")).isEqualTo(1);
    }

    @Test
    void expiredPaymentDoesNotCancelAndItsResultReplaysAfterCancellation() throws Exception {
        long campaign = campaign(1);
        String order = order(campaign, USER);
        expireCampaign(campaign, 1);
        Reply expired = send(payment(order, USER, "expired"));
        assertRejected(expired, "ORDER_EXPIRED");
        assertThat(orderStatus(order)).isEqualTo("PENDING");
        assertThat(attempts(order, "REJECTED_EXPIRED")).isEqualTo(1);
        assertReconciliation(campaign, 0, 1, 0);

        assertThat(expiry.cancelExpiredBatch()).isEqualTo(1);
        assertThat(orderStatus(order)).isEqualTo("CANCELLED");
        assertThat(events(order, "ORDER_CANCELLED")).isEqualTo(1);
        assertReconciliation(campaign, 1, 0, 0);
        Reply replay = send(payment(order, USER, "expired"));
        assertThat(replay.status()).isEqualTo(expired.status());
        assertThat(replay.body()).isEqualTo(expired.body());
        assertThat(replay.replay()).isEqualTo("true");
    }

    @Test
    void cancelledOrderRejectsPaymentWithoutChangingStock() throws Exception {
        long campaign = campaign(1);
        String order = order(campaign, USER);
        expireCampaign(campaign, 1);
        assertThat(expiry.cancelExpiredBatch()).isEqualTo(1);
        Reply rejected = send(payment(order, USER, "cancelled"));
        assertRejected(rejected, "ORDER_CANCELLED");
        assertThat(orderStatus(order)).isEqualTo("CANCELLED");
        assertThat(attempts(order, "REJECTED_CANCELLED")).isEqualTo(1);
        assertThat(events(order, "ORDER_PAID")).isZero();
        assertReconciliation(campaign, 1, 0, 0);
    }

    @Test
    void paymentThatWaitsOnAnOrderLockRechecksDatabaseTimeAfterDeadline() throws Exception {
        long campaign = campaign(1);
        String order = order(campaign, USER);
        jdbc.update("UPDATE purchase_order SET expire_at = NOW(6) + INTERVAL 2 SECOND WHERE order_uid = ?", order);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Reply> payment;
            try (Connection blocker = lockRow("SELECT id FROM purchase_order WHERE order_uid = ? FOR UPDATE", order)) {
                payment = executor.submit(() -> send(payment(order, USER, "lock-crosses-deadline")));
                await(() -> runningStatements("from purchase_order", "for update") > 0);
                assertThat(count("SELECT COUNT(*) FROM purchase_order WHERE order_uid = ? AND expire_at > NOW(6)",
                        order)).isEqualTo(1);
                await(() -> count("SELECT COUNT(*) FROM purchase_order WHERE order_uid = ? AND expire_at <= NOW(6)",
                        order) == 1);
                assertThat(payment.isDone()).isFalse();
            }
            assertRejected(payment.get(10, TimeUnit.SECONDS), "ORDER_EXPIRED");
        }
        assertThat(orderStatus(order)).isEqualTo("PENDING");
        assertThat(attempts(order, "REJECTED_EXPIRED")).isEqualTo(1);
        assertThat(events(order, "ORDER_PAID")).isZero();
        assertThat(expiry.cancelExpiredBatch()).isEqualTo(1);
    }

    @Test
    void twoExpiryWorkersSkipEachOthersLocksAndReturnStockExactlyOnce() throws Exception {
        long first = campaign(50);
        long second = campaign(50);
        var firstOrders = orders(first, 50);
        var secondOrders = orders(second, 50);
        jdbc.update("""
                UPDATE purchase_order
                   SET expire_at = TIMESTAMPADD(SECOND, user_id - 1100, NOW(6))
                 WHERE campaign_id IN (?, ?)
                """, first, second);
        var cancelled = meters.counter("flashorder.orders.cancelled");
        double cancelledBefore = cancelled.count();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Integer> one;
            Future<Integer> two;
            try (Connection blocker = lockRow("SELECT available FROM inventory WHERE campaign_id = ? FOR UPDATE", first)) {
                one = executor.submit(this::drainExpired);
                two = executor.submit(this::drainExpired);
                // Both workers reached the stock update for different locked orders in the same campaign.
                await(() -> runningStatements("update inventory", "available") == 2);
            }
            int oneCount = one.get(20, TimeUnit.SECONDS);
            int twoCount = two.get(20, TimeUnit.SECONDS);
            assertThat(oneCount).isGreaterThanOrEqualTo(25);
            assertThat(twoCount).isGreaterThanOrEqualTo(25);
            assertThat(oneCount + twoCount).isEqualTo(100);
        }

        for (String order : Stream.concat(firstOrders.stream(), secondOrders.stream()).toList()) {
            assertThat(orderStatus(order)).isEqualTo("CANCELLED");
            assertThat(events(order, "ORDER_CANCELLED")).isEqualTo(1);
        }
        assertReconciliation(first, 50, 0, 0);
        assertReconciliation(second, 50, 0, 0);
        assertThat(cancelled.count()).isEqualTo(cancelledBefore + 100);
    }

    @Test
    void concurrentPaymentAndCancellationHaveOneTerminalOutcomePerOrder() throws Exception {
        long campaign = campaign(50);
        var orders = orders(campaign, 50);
        jdbc.update("UPDATE purchase_order SET expire_at = NOW(6) + INTERVAL 5 SECOND WHERE campaign_id = ?",
                campaign);

        var replies = new ArrayList<Reply>(50);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var beforeDeadline = new CountDownLatch(1);
            var early = race(executor, beforeDeadline, 25, i -> payment(orders.get(i), 1000 + i, "race"));
            beforeDeadline.countDown();
            replies.addAll(collect(early));
            assertThat(replies).allSatisfy(reply -> assertThat(reply.status()).isEqualTo(200));
            await(() -> count("SELECT COUNT(*) FROM purchase_order WHERE campaign_id = ? AND expire_at <= NOW(6)",
                    campaign) == 50);

            List<Future<Reply>> late;
            // Lock one expired order first to prove the concurrent cancellation skips it.
            try (Connection heldOrder = lockRow("SELECT id FROM purchase_order WHERE order_uid = ? FOR UPDATE",
                    orders.getLast())) {
                Future<Integer> cancellation;
                try (Connection blocker = lockRow("SELECT available FROM inventory WHERE campaign_id = ? FOR UPDATE",
                        campaign);
                     Connection observer = dataSource.getConnection()) {
                    JdbcTemplate probe = new JdbcTemplate(new SingleConnectionDataSource(observer, true));
                    cancellation = executor.submit(expiry::cancelExpiredBatch);
                    await(() -> runningStatements(probe, "update inventory", "available") == 1);
                    var afterDeadline = new CountDownLatch(1);
                    late = race(executor, afterDeadline, 25, i -> payment(orders.get(i + 25), 1025 + i, "race"));
                    afterDeadline.countDown();
                    await(() -> runningStatements(probe, "from purchase_order", "for update") > 0);
                }
                assertThat(cancellation.get(15, TimeUnit.SECONDS)).isEqualTo(24);
                assertThat(orderStatus(orders.getLast())).isEqualTo("PENDING");
            }
            replies.addAll(collect(late));
        }
        drainExpired();

        int paid = 0;
        for (int index = 0; index < orders.size(); index++) {
            String order = orders.get(index);
            Reply reply = replies.get(index);
            if (reply.status() == 200) {
                assertPaid(reply, order);
                assertThat(orderStatus(order)).isEqualTo("PAID");
                assertThat(events(order, "ORDER_PAID")).isEqualTo(1);
                assertThat(events(order, "ORDER_CANCELLED")).isZero();
                paid++;
            } else {
                assertThat(reply.status()).isEqualTo(409);
                assertThat(reply.body().get("reason").asString()).isIn("ORDER_EXPIRED", "ORDER_CANCELLED");
                assertThat(orderStatus(order)).isEqualTo("CANCELLED");
                assertThat(events(order, "ORDER_PAID")).isZero();
                assertThat(events(order, "ORDER_CANCELLED")).isEqualTo(1);
            }
        }
        assertThat(paid).isEqualTo(25);
        assertReconciliation(campaign, 50 - paid, 0, paid);
    }

    @Test
    void cancellationMakesUserEligibleAgainAndOriginalRequestStillReplays() throws Exception {
        long campaign = campaign(1);
        HttpRequest original = purchase(campaign, USER, "original");
        Reply old = send(original);
        assertThat(old.status()).isEqualTo(201);
        expireCampaign(campaign, 1);
        assertThat(expiry.cancelExpiredBatch()).isEqualTo(1);

        Reply replacement = send(purchase(campaign, USER, "replacement"));
        assertThat(replacement.status()).isEqualTo(201);
        assertThat(replacement.body().get("orderUid")).isNotEqualTo(old.body().get("orderUid"));
        Reply replay = send(original);
        assertThat(replay.status()).isEqualTo(201);
        assertThat(replay.body()).isEqualTo(old.body());
        assertThat(replay.replay()).isEqualTo("true");
        assertReconciliation(campaign, 0, 1, 0);
    }

    @Test
    void cancellationLeavesUnexpiredPendingAndPaidOrdersAlone() throws Exception {
        long campaign = campaign(3);
        String pending = order(campaign, USER);
        String paid = order(campaign, OTHER_USER);
        String expired = order(campaign, 103);
        assertPaid(send(payment(paid, OTHER_USER, "pay")), paid);
        jdbc.update("""
                UPDATE purchase_order SET expire_at = NOW(6) - INTERVAL 1 MINUTE
                 WHERE order_uid IN (?, ?)
                """, paid, expired);

        assertThat(expiry.cancelExpiredBatch()).isEqualTo(1);
        assertThat(orderStatus(pending)).isEqualTo("PENDING");
        assertThat(orderStatus(paid)).isEqualTo("PAID");
        assertThat(orderStatus(expired)).isEqualTo("CANCELLED");
        assertThat(events(pending, "ORDER_CANCELLED")).isZero();
        assertThat(events(paid, "ORDER_CANCELLED")).isZero();
        assertReconciliation(campaign, 1, 1, 1);
    }

    @Test
    void reconciliationReportsStockAndRequestLinkDriftWithoutRepairingIt() throws Exception {
        long campaign = campaign(3);
        String pending = order(campaign, USER);
        String paid = order(campaign, OTHER_USER);
        assertPaid(send(payment(paid, OTHER_USER, "pay")), paid);
        assertReconciliation(campaign, 1, 1, 1);
        long pendingOrderId = count("SELECT id FROM purchase_order WHERE order_uid = ?", pending);
        long paidOrderId = count("SELECT id FROM purchase_order WHERE order_uid = ?", paid);

        try {
            jdbc.update("UPDATE inventory SET available = 0 WHERE campaign_id = ?", campaign);
            jdbc.update("UPDATE purchase_request SET order_id = NULL WHERE order_id = ?", pendingOrderId);
            jdbc.update("UPDATE purchase_request SET status = 'REJECTED' WHERE order_id = ?", paidOrderId);
            JsonNode report = reconcile(campaign);
            assertThat(report.get("gap").asLong()).isEqualTo(1);
            assertThat(report.get("createdWithoutOrder").asLong()).isEqualTo(1);
            assertThat(report.get("nonCreatedWithOrder").asLong()).isEqualTo(1);
            assertThat(report.get("consistent").asBoolean()).isFalse();
            assertThat(report.get("sameSnapshot").asBoolean()).isFalse();
            assertThat(meters.get("flashorder.reconciliation.gap").tag("campaign", Long.toString(campaign))
                    .gauge().value()).isEqualTo(1);
            assertThat(count("SELECT available FROM inventory WHERE campaign_id = ?", campaign)).isZero();
        } finally {
            jdbc.update("UPDATE inventory SET available = 1 WHERE campaign_id = ?", campaign);
            jdbc.update("""
                    UPDATE purchase_request r JOIN purchase_order o ON o.request_id = r.id
                       SET r.status = 'ORDER_CREATED', r.order_id = o.id
                     WHERE o.id IN (?, ?)
                    """, pendingOrderId, paidOrderId);
        }
    }

    @Test
    void ownershipAndInvalidInputLeavePaymentUntouched() throws Exception {
        String order = order(campaign(1), USER);
        assertThat(send(payment(order, OTHER_USER, "unauthorized")).status()).isEqualTo(404);
        assertThat(send(payment(order, USER, "bad key")).status()).isEqualTo(400);
        assertThat(send(payment(order, 0, "bad-user")).status()).isEqualTo(400);
        assertThat(send(get("/admin/campaigns/" + Long.MAX_VALUE + "/reconciliation")).status()).isEqualTo(404);
        assertThat(orderStatus(order)).isEqualTo("PENDING");
        assertThat(events(order, "ORDER_PAID")).isZero();
        assertThat(count("""
                SELECT COUNT(*) FROM payment_attempt p
                JOIN purchase_order o ON p.order_id = o.id WHERE o.order_uid = ?
                """, order)).isZero();
    }

    private int drainExpired() {
        int total = 0;
        int cancelled;
        do {
            cancelled = expiry.cancelExpiredBatch();
            total += cancelled;
        } while (cancelled > 0);
        return total;
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

    private String order(long campaign, long user) throws Exception {
        return createdOrder(send(purchase(campaign, user, "buy-" + campaign)));
    }

    private List<String> orders(long campaign, int count) throws Exception {
        var prepared = new ArrayList<String>(count);
        // Prepare the fixture without competing for the same inventory row before the race under test.
        for (int index = 0; index < count; index++) {
            prepared.add(order(campaign, 1000 + index));
        }
        return prepared;
    }

    private static String createdOrder(Reply reply) {
        assertThat(reply.status()).as("%s", reply.body()).isEqualTo(201);
        return reply.body().get("orderUid").asString();
    }

    private void expireCampaign(long campaign, int minutesAgo) {
        jdbc.update("UPDATE purchase_order SET expire_at = TIMESTAMPADD(MINUTE, ?, NOW(6)) WHERE campaign_id = ?",
                -minutesAgo, campaign);
    }

    private String orderStatus(String order) {
        return jdbc.queryForObject("SELECT status FROM purchase_order WHERE order_uid = ?", String.class, order);
    }

    private long count(String sql, Object... arguments) {
        return jdbc.queryForObject(sql, Long.class, arguments);
    }

    private long attempts(String order, String result) {
        return count("""
                SELECT COUNT(*) FROM payment_attempt p
                JOIN purchase_order o ON p.order_id = o.id
                WHERE o.order_uid = ? AND p.result = ?
                """, order, result);
    }

    private long events(String order, String type) {
        return count("SELECT COUNT(*) FROM outbox_message WHERE msg_key = ? AND event_type = ?", order, type);
    }

    private double paymentReplayCount() {
        return meters.counter("flashorder.payments", "result", "SUCCEEDED", "replayed", "true").count();
    }

    private Connection lockRow(String query, Object key) throws Exception {
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement(query)) {
                statement.setObject(1, key);
                try (var rows = statement.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                }
            }
            return connection;
        } catch (Exception | AssertionError failure) {
            connection.close();
            throw failure;
        }
    }

    private long runningStatements(String firstFragment, String secondFragment) {
        return runningStatements(jdbc, firstFragment, secondFragment);
    }

    private static long runningStatements(JdbcTemplate probe, String firstFragment, String secondFragment) {
        return probe.queryForList("SHOW FULL PROCESSLIST").stream()
                .map(row -> row.get("Info"))
                .filter(String.class::isInstance)
                .map(info -> ((String) info).toLowerCase(Locale.ROOT))
                .filter(info -> info.contains(firstFragment) && info.contains(secondFragment))
                .count();
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(6);
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime()).as("MySQL condition did not become true").isLessThan(deadline);
            Thread.sleep(20);
        }
    }

    private List<Future<Reply>> race(ExecutorService executor, CountDownLatch start, int count,
                                     IntFunction<HttpRequest> requests) {
        return IntStream.range(0, count)
                .mapToObj(index -> executor.submit(() -> {
                    if (!start.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("HTTP race did not start");
                    }
                    return send(requests.apply(index));
                })).toList();
    }

    private static List<Reply> collect(List<Future<Reply>> results) throws Exception {
        var replies = new ArrayList<Reply>(results.size());
        for (Future<Reply> result : results) {
            replies.add(result.get(30, TimeUnit.SECONDS));
        }
        return replies;
    }

    private HttpRequest purchase(long campaign, long user, String key) {
        return post("/campaigns/" + campaign + "/purchase-requests", user, key);
    }

    private HttpRequest payment(String order, long user, String key) {
        return post("/orders/" + order + "/payments", user, key);
    }

    private HttpRequest post(String path, long user, String key) {
        return request(path)
                .header("X-User-Id", Long.toString(user))
                .header("Idempotency-Key", prefix + key)
                .POST(HttpRequest.BodyPublishers.noBody()).build();
    }

    private HttpRequest get(String path) {
        return request(path).GET().build();
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).timeout(Duration.ofSeconds(20));
    }

    private Reply send(HttpRequest request) throws Exception {
        var reply = client.send(request, HttpResponse.BodyHandlers.ofString());
        return new Reply(reply.statusCode(), reply.headers().firstValue("Idempotent-Replayed").orElse(null),
                reply.body().isBlank() ? null : JSON.readTree(reply.body()));
    }

    private JsonNode reconcile(long campaign) throws Exception {
        Reply reply = send(get("/admin/campaigns/" + campaign + "/reconciliation"));
        assertThat(reply.status()).isEqualTo(200);
        return reply.body();
    }

    private void assertReconciliation(long campaign, int available, int pending, int paid) throws Exception {
        JsonNode report = reconcile(campaign);
        assertThat(report.get("available").asInt()).isEqualTo(available);
        assertThat(report.get("pending").asInt()).isEqualTo(pending);
        assertThat(report.get("paid").asInt()).isEqualTo(paid);
        assertThat(report.get("gap").asLong()).isZero();
        assertThat(report.get("consistent").asBoolean()).isTrue();
    }

    private static void assertPaid(Reply reply, String order) {
        assertThat(reply.status()).as("%s", reply.body()).isEqualTo(200);
        assertThat(reply.body().get("orderUid").asString()).isEqualTo(order);
        assertThat(reply.body().get("status").asString()).isEqualTo("PAID");
        assertThat(OffsetDateTime.parse(reply.body().get("paidAt").asString()).getOffset()).isEqualTo(ZoneOffset.UTC);
    }

    private static void assertRejected(Reply reply, String reason) {
        assertThat(reply.status()).as("%s", reply.body()).isEqualTo(409);
        assertThat(reply.body().get("status").asString()).isEqualTo("REJECTED");
        assertThat(reply.body().get("reason").asString()).isEqualTo(reason);
    }

    private record Reply(int status, String replay, JsonNode body) {}
}
