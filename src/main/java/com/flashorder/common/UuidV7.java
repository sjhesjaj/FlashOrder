package com.flashorder.common;

import java.security.SecureRandom;
import java.util.UUID;

public final class UuidV7 {
    private static final SecureRandom RANDOM = new SecureRandom();

    private UuidV7() {
    }

    public static String next() {
        // UUIDv7 stores the millisecond timestamp before the version and random bits.
        long mostSignificant = (System.currentTimeMillis() << 16) | 0x7000L | RANDOM.nextInt(1 << 12);
        long leastSignificant = (RANDOM.nextLong() & 0x3fff_ffff_ffff_ffffL) | Long.MIN_VALUE;
        return new UUID(mostSignificant, leastSignificant).toString();
    }
}
