package com.flashorder;

import com.flashorder.request.guard.CampaignProtectionSnapshot;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ActiveProfiles("api")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "flashorder.intake-mode=async",
        "flashorder.protection.enabled=true",
        "flashorder.protection.backlog-limit=4",
        "flashorder.protection.rate-limit.instances=4",
        "flashorder.protection.rate-limit.user-rate-per-second=1",
        "flashorder.protection.rate-limit.user-burst=4",
        "flashorder.protection.rate-limit.campaign-rate-per-second=1",
        "flashorder.protection.rate-limit.campaign-burst=4",
        "spring.data.redis.connect-timeout=1s",
        "spring.data.redis.timeout=1s"
})
@DirtiesContext
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PurchaseProtectionIntegrationTest {
    private static final int REDIS_PORT = 6379;
    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4.12-alpine")).withExposedPorts(REDIS_PORT);

    private final HttpClient client = HttpClient.newHttpClient();

    @LocalServerPort
    private int port;
    @Autowired
    private CampaignProtectionSnapshot snapshot;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private StringRedisTemplate redis;
    @Autowired
    private MeterRegistry meters;

    @DynamicPropertySource
    static void configureInfrastructure(DynamicPropertyRegistry properties) {
        var mysql = MySqlTestDatabase.startMysql("flashorder_protection_test");
        REDIS.start();
        properties.add("spring.datasource.url", mysql::getJdbcUrl);
        properties.add("spring.datasource.username", mysql::getUsername);
        properties.add("spring.datasource.password", mysql::getPassword);
        properties.add("spring.data.redis.host", REDIS::getHost);
        properties.add("spring.data.redis.port", () -> REDIS.getMappedPort(REDIS_PORT));
    }

    @AfterEach
    void closeClient() {
        client.close();
    }

    @Test
    @Order(1)
    void redisLuaEnforcesBothBurstsThroughHttp() throws Exception {
        long userCampaign = campaign(10);
        assertAccepted(submit(userCampaign, 10));
        freezeRefill("user", 10);
        for (int request = 0; request < 3; request++) {
            assertAccepted(submit(userCampaign, 10));
        }
        assertRejected(submit(userCampaign, 10), 429, "USER_RATE_LIMITED");
        assertAdmissionCount(userCampaign, 4);

        long rateLimitedCampaign = campaign(10);
        assertAccepted(submit(rateLimitedCampaign, 20));
        freezeRefill("campaign", rateLimitedCampaign);
        for (long user = 21; user <= 23; user++) {
            assertAccepted(submit(rateLimitedCampaign, user));
        }
        assertRejected(submit(rateLimitedCampaign, 24), 429, "CAMPAIGN_RATE_LIMITED");
        assertAdmissionCount(rateLimitedCampaign, 4);

        assertThat(decisions("user", "redis", "deny")).isEqualTo(1);
        assertThat(decisions("campaign", "redis", "deny")).isEqualTo(1);
    }

    @Test
    @Order(2)
    void soldOutSnapshotRejectsWithoutAcceptingTheRequest() throws Exception {
        long soldOutCampaign = campaign(0);
        await().atMost(TIMEOUT).until(() -> "SOLD_OUT".equals(snapshot.fastRejectReason(soldOutCampaign)));

        assertRejected(submit(soldOutCampaign, 30), 409, "SOLD_OUT");
        assertAdmissionCount(soldOutCampaign, 0);
    }

    @Test
    @Order(3)
    void backlogRejectsNewRequestsButAllowsAnIdempotentReplay() throws Exception {
        long campaign = campaign(10);
        String originalKey = UUID.randomUUID().toString();
        Reply original = submit(campaign, 40, originalKey);
        assertAccepted(original);
        for (long user = 41; user <= 43; user++) {
            assertAccepted(submit(campaign, user));
        }
        await().atMost(TIMEOUT).until(() -> snapshot.backlogFull(campaign));

        // Give the new request a campaign permit so it reaches the backlog check.
        redis.delete(bucketKey("campaign", campaign));
        assertRejected(submit(campaign, 44), 503, "IN_FLIGHT_FULL");
        Reply replay = submit(campaign, 40, originalKey);
        assertAccepted(replay);
        assertThat(replay.replayed()).isEqualTo("true");
        assertThat(replay.body()).isEqualTo(original.body());
        assertAdmissionCount(campaign, 4);
    }

    @Test
    @Order(4) // This test stops the shared test-owned Redis container.
    void redisShutdownUsesScaledLocalUserAndCampaignBuckets() throws Exception {
        double failuresBefore = meters.counter("flashorder.rate.limit.redis.failures").count();
        long campaign = campaign(10);
        REDIS.stop();

        assertAccepted(submit(campaign, 50));
        assertRejected(submit(campaign, 50), 429, "USER_RATE_LIMITED");
        assertRejected(submit(campaign, 51), 429, "CAMPAIGN_RATE_LIMITED");
        assertAdmissionCount(campaign, 1);

        assertThat(meters.counter("flashorder.rate.limit.redis.failures").count()).isGreaterThan(failuresBefore);
        assertThat(decisions("user", "local", "deny")).isEqualTo(1);
        assertThat(decisions("campaign", "local", "deny")).isEqualTo(1);
    }

    private void freezeRefill(String scope, long id) {
        String key = bucketKey(scope, id);
        var fields = redis.<String, String>opsForHash();
        assertThat(fields.get(key, "tokens")).isEqualTo("3");
        long updatedAt = Long.parseLong(fields.get(key, "updated_at"));
        // Simulate a clock regression so host pauses cannot refill the burst between HTTP requests.
        fields.put(key, "updated_at", Long.toString(updatedAt + 60_000_000L));
        redis.expire(key, Duration.ofMinutes(1));
    }

    private static String bucketKey(String scope, long id) {
        return "flashorder:rate-limit:" + scope + ":" + id;
    }

    private double decisions(String scope, String backend, String result) {
        return meters.counter("flashorder.rate.limit.decisions",
                "scope", scope, "backend", backend, "result", result).count();
    }

    private long campaign(int stock) {
        String sku = UUID.randomUUID().toString();
        jdbc.update("""
                INSERT INTO campaign (sku, starts_at, ends_at)
                VALUES (?, NOW(6) - INTERVAL 1 HOUR, NOW(6) + INTERVAL 1 HOUR)
                """, sku);
        long campaign = jdbc.queryForObject("SELECT id FROM campaign WHERE sku = ?", Long.class, sku);
        jdbc.update("INSERT INTO inventory (campaign_id, total, available) VALUES (?, ?, ?)", campaign, stock, stock);
        return campaign;
    }

    private void assertAdmissionCount(long campaign, int expected) {
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM purchase_request WHERE campaign_id = ?",
                Integer.class, campaign)).isEqualTo(expected);
        assertThat(jdbc.queryForObject("""
                SELECT COUNT(*) FROM outbox_message
                WHERE msg_key = ? AND event_type = 'CREATE_ORDER' AND published_at IS NULL
                """, Integer.class, Long.toString(campaign))).isEqualTo(expected);
    }

    private Reply submit(long campaign, long user) throws Exception {
        return submit(campaign, user, UUID.randomUUID().toString());
    }

    private Reply submit(long campaign, long user, String key) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                        + "/campaigns/" + campaign + "/purchase-requests"))
                .timeout(TIMEOUT).header("X-User-Id", Long.toString(user)).header("Idempotency-Key", key)
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return new Reply(response.statusCode(), response.headers().firstValue("Retry-After").orElse(null),
                response.headers().firstValue("Idempotent-Replayed").orElse(null), JSON.readTree(response.body()));
    }

    private static void assertAccepted(Reply reply) {
        assertThat(reply.status()).isEqualTo(202);
        assertThat(reply.body().get("status").asString()).isEqualTo("ACCEPTED");
        assertThat(reply.body().hasNonNull("requestUid")).isTrue();
    }

    private static void assertRejected(Reply reply, int httpStatus, String reason) {
        assertThat(reply.status()).isEqualTo(httpStatus);
        assertThat(reply.retryAfter()).isEqualTo("1");
        assertThat(reply.body().get("status").asString()).isEqualTo("NOT_ACCEPTED");
        assertThat(reply.body().get("reason").asString()).isEqualTo(reason);
        assertThat(reply.body().has("requestUid")).isFalse();
    }

    private record Reply(int status, String retryAfter, String replayed, JsonNode body) {
    }
}
