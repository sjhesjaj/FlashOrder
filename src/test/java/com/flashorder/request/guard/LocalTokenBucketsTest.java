package com.flashorder.request.guard;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class LocalTokenBucketsTest {
    @Test
    void fractionalCapacityAndRefillDoNotRoundUpTheLocalQuota() {
        AtomicLong time = new AtomicLong();
        LocalTokenBuckets buckets = new LocalTokenBuckets(1.5, 2.5, 10, time::get);

        assertThat(buckets.allow(7)).isTrue();
        assertThat(buckets.allow(7)).isTrue();
        assertThat(buckets.allow(7)).isFalse();

        time.set(Duration.ofMillis(333).toNanos());
        assertThat(buckets.allow(7)).isFalse();
        time.set(Duration.ofMillis(334).toNanos());
        assertThat(buckets.allow(7)).isTrue();
        assertThat(buckets.allow(7)).isFalse();
    }

    @Test
    void saturatedMapKeepsLiveStateAndReusesSlotsOnlyAfterFullRefill() {
        AtomicLong time = new AtomicLong();
        LocalTokenBuckets buckets = new LocalTokenBuckets(1, 2, 1, time::get);

        assertThat(buckets.allow(7)).isTrue();
        assertThat(buckets.allow(7)).isTrue();
        assertThat(buckets.allow(8)).isFalse();

        time.set(Duration.ofMillis(1999).toNanos());
        assertThat(buckets.allow(8)).isFalse();
        time.set(Duration.ofSeconds(2).toNanos());
        assertThat(buckets.allow(8)).isTrue();
        assertThat(buckets.allow(7)).isFalse();
    }

    @Test
    void simultaneousCallsCannotSpendTheSameToken() {
        LocalTokenBuckets buckets = new LocalTokenBuckets(1, 5, 10, () -> 0L);
        long allowed = IntStream.range(0, 40).parallel().filter(ignored -> buckets.allow(7)).count();

        assertThat(allowed).isEqualTo(5);
    }
}
