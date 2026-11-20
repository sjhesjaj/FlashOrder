package com.flashorder.request.guard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

class PurchaseRateLimiterTest {
    @Test
    void keepsRedisKeysAndQuotasSeparateForTheTwoScopes() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        PurchaseRateLimiter limits = new PurchaseRateLimiter(redis, meters, settings(), () -> 0L);
        when(redis.execute(ArgumentMatchers.<RedisScript<Long>>any(),
                eq(List.of("flashorder:rate-limit:user:7")), eq("5"), eq("5"))).thenReturn(0L);
        when(redis.execute(ArgumentMatchers.<RedisScript<Long>>any(),
                eq(List.of("flashorder:rate-limit:campaign:7")), eq("200"), eq("20"))).thenReturn(1L);

        assertThat(limits.allowUser(7)).isFalse();
        assertThat(limits.allowCampaign(7)).isTrue();
        assertThat(meters.get("flashorder.rate.limit.decisions")
                .tags("scope", "user", "backend", "redis", "result", "deny").counter().count()).isEqualTo(1);
        assertThat(meters.get("flashorder.rate.limit.redis.failures").counter().count()).isZero();
    }

    @Test
    void usesDividedLocalQuotaDuringOutageAndReturnsToRedisAfterProbe() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        AtomicLong time = new AtomicLong();
        PurchaseRateLimiter limits = new PurchaseRateLimiter(redis, meters, settings(), time::get);
        when(redis.execute(ArgumentMatchers.<RedisScript<Long>>any(),
                eq(List.of("flashorder:rate-limit:user:7")), eq("5"), eq("5")))
                .thenThrow(new DataAccessResourceFailureException("Redis offline"))
                .thenReturn(0L, 1L);

        assertThat(limits.allowUser(7)).isTrue();
        assertThat(limits.allowUser(7)).isTrue();
        assertThat(limits.allowUser(7)).isFalse();
        assertThat(meters.get("flashorder.rate.limit.decisions")
                .tags("scope", "user", "backend", "local", "result", "deny").counter().count()).isEqualTo(1);
        assertThat(meters.get("flashorder.rate.limit.redis.failures").counter().count()).isEqualTo(1);

        time.set(Duration.ofSeconds(1).toNanos());
        assertThat(limits.allowUser(7)).isFalse();
        assertThat(limits.allowUser(7)).isTrue();
        verify(redis, times(3)).execute(ArgumentMatchers.<RedisScript<Long>>any(),
                eq(List.of("flashorder:rate-limit:user:7")), eq("5"), eq("5"));
    }

    @Test
    void rejectsSettingsWhoseDividedCapacityWouldRemainBelowOneToken() {
        assertThatIllegalArgumentException().isThrownBy(
                () -> new PurchaseRateLimiter.Settings(5, 1, 200, 20, 2, 10, 1000));
    }

    private static PurchaseRateLimiter.Settings settings() {
        return new PurchaseRateLimiter.Settings(5, 5, 200, 20, 2, 10, 1000);
    }
}
