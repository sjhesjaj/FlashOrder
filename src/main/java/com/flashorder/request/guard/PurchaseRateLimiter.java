package com.flashorder.request.guard;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

@Component
@Profile("api")
@ConditionalOnProperty(name = "flashorder.protection.enabled", havingValue = "true")
public class PurchaseRateLimiter {
    private static final RedisScript<Long> TOKEN_BUCKET =
            RedisScript.of(new ClassPathResource("redis/token-bucket.lua"), Long.class);

    private final StringRedisTemplate redis;
    private final MeterRegistry meters;
    private final Counter redisFailures;
    private final ScopeLimit user;
    private final ScopeLimit campaign;
    private final long retryNanos;
    private final LongSupplier nanoTime;
    private final AtomicReference<RedisPause> redisPause = new AtomicReference<>();
    private final AtomicBoolean probingRedis = new AtomicBoolean();

    @Autowired
    public PurchaseRateLimiter(
            StringRedisTemplate redis, MeterRegistry meters,
            @Value("${flashorder.protection.rate-limit.user-rate-per-second:5}") int userRate,
            @Value("${flashorder.protection.rate-limit.user-burst:10}") int userBurst,
            @Value("${flashorder.protection.rate-limit.campaign-rate-per-second:200}") int campaignRate,
            @Value("${flashorder.protection.rate-limit.campaign-burst:20}") int campaignBurst,
            @Value("${flashorder.protection.rate-limit.instances:1}") int instances,
            @Value("${flashorder.protection.rate-limit.max-local-buckets:10000}") int maxLocalBuckets,
            @Value("${flashorder.protection.rate-limit.redis-retry-ms:1000}") int redisRetryMs) {
        this(redis, meters, new Settings(userRate, userBurst, campaignRate, campaignBurst,
                instances, maxLocalBuckets, redisRetryMs), System::nanoTime);
    }

    PurchaseRateLimiter(StringRedisTemplate redis, MeterRegistry meters, Settings settings, LongSupplier nanoTime) {
        this.redis = redis;
        this.meters = meters;
        this.redisFailures = meters.counter("flashorder.rate.limit.redis.failures");
        this.nanoTime = nanoTime;
        this.retryNanos = TimeUnit.MILLISECONDS.toNanos(settings.redisRetryMs());
        this.user = scopeLimit("user", settings.userRate(), settings.userBurst(), settings);
        this.campaign = scopeLimit("campaign", settings.campaignRate(), settings.campaignBurst(), settings);
    }

    public boolean allowUser(long userId) {
        return allow(user, userId);
    }

    public boolean allowCampaign(long campaignId) {
        return allow(campaign, campaignId);
    }

    private boolean allow(ScopeLimit limit, long id) {
        RedisPause pause = redisPause.get();
        if (pause == null) {
            return redisOrLocal(limit, id, null);
        }
        if (nanoTime.getAsLong() - pause.retryAtNanos() < 0 || !probingRedis.compareAndSet(false, true)) {
            return allowLocally(limit, id);
        }
        try {
            return redisOrLocal(limit, id, pause);
        } finally {
            probingRedis.set(false);
        }
    }

    private boolean redisOrLocal(ScopeLimit limit, long id, RedisPause pause) {
        Long decision;
        try {
            decision = redis.execute(TOKEN_BUCKET, List.of(limit.keyPrefix() + id), limit.rate(), limit.burst());
        } catch (DataAccessException failure) {
            return useFallback(limit, id);
        }
        if (decision == null || (decision != 0 && decision != 1)) {
            return useFallback(limit, id);
        }
        if (pause != null) {
            redisPause.compareAndSet(pause, null);
        }
        return record(limit, "redis", decision == 1);
    }

    private boolean useFallback(ScopeLimit limit, long id) {
        redisFailures.increment();
        redisPause.set(new RedisPause(nanoTime.getAsLong() + retryNanos));
        return allowLocally(limit, id);
    }

    private boolean allowLocally(ScopeLimit limit, long id) {
        return record(limit, "local", limit.local().allow(id));
    }

    private boolean record(ScopeLimit limit, String backend, boolean allowed) {
        meters.counter("flashorder.rate.limit.decisions", "scope", limit.scope(), "backend", backend,
                "result", allowed ? "allow" : "deny").increment();
        return allowed;
    }

    private ScopeLimit scopeLimit(String scope, int rate, int burst, Settings settings) {
        int instances = settings.instances();
        return new ScopeLimit(scope, "flashorder:rate-limit:" + scope + ":", Integer.toString(rate),
                Integer.toString(burst), new LocalTokenBuckets(rate / (double) instances,
                burst / (double) instances, settings.maxLocalBuckets(), nanoTime));
    }

    record Settings(int userRate, int userBurst, int campaignRate, int campaignBurst,
                    int instances, int maxLocalBuckets, int redisRetryMs) {
        Settings {
            if (userRate < 1 || campaignRate < 1 || instances < 1 || maxLocalBuckets < 1 || redisRetryMs < 1) {
                throw new IllegalArgumentException("Rate limits, instance count, fallback size and retry must be positive");
            }
            if (userBurst < instances || campaignBurst < instances) {
                throw new IllegalArgumentException("Each rate limit burst must be at least the API instance count");
            }
        }
    }

    private record ScopeLimit(String scope, String keyPrefix, String rate, String burst, LocalTokenBuckets local) {
    }

    private record RedisPause(long retryAtNanos) {
    }
}
