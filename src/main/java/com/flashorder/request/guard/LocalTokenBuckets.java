package com.flashorder.request.guard;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

final class LocalTokenBuckets {
    private final double ratePerSecond;
    private final double capacity;
    private final int maxBuckets;
    private final long expirationNanos;
    private final LongSupplier nanoTime;
    private final LinkedHashMap<Long, Bucket> buckets = new LinkedHashMap<>(16, 0.75f, true);

    LocalTokenBuckets(double ratePerSecond, double capacity, int maxBuckets, LongSupplier nanoTime) {
        this.ratePerSecond = ratePerSecond;
        this.capacity = capacity;
        this.maxBuckets = maxBuckets;
        this.expirationNanos = Math.max(TimeUnit.SECONDS.toNanos(1),
                (long) Math.ceil(capacity / ratePerSecond * TimeUnit.SECONDS.toNanos(1)));
        this.nanoTime = nanoTime;
    }

    synchronized boolean allow(long key) {
        long now = nanoTime.getAsLong();
        evictIdleBuckets(now);

        Bucket bucket = buckets.get(key);
        if (bucket == null) {
            // An active bucket must keep its state even when new IDs exhaust the map.
            if (buckets.size() == maxBuckets) {
                return false;
            }
            bucket = new Bucket(capacity, now);
            buckets.put(key, bucket);
        }

        double elapsedSeconds = (now - bucket.updatedAt) / (double) TimeUnit.SECONDS.toNanos(1);
        bucket.tokens = Math.min(capacity, bucket.tokens + elapsedSeconds * ratePerSecond);
        bucket.updatedAt = now;
        if (bucket.tokens < 1) {
            return false;
        }
        bucket.tokens--;
        return true;
    }

    private void evictIdleBuckets(long now) {
        Iterator<Bucket> oldest = buckets.values().iterator();
        while (oldest.hasNext() && now - oldest.next().updatedAt >= expirationNanos) {
            oldest.remove();
        }
    }

    private static final class Bucket {
        private double tokens;
        private long updatedAt;

        private Bucket(double tokens, long updatedAt) {
            this.tokens = tokens;
            this.updatedAt = updatedAt;
        }
    }
}
