package com.flashorder.inventory;

public record InventoryReconciliation(
        long campaignId,
        int total,
        int available,
        long pending,
        long paid,
        long gap,
        long createdWithoutOrder,
        long nonCreatedWithOrder,
        boolean consistent,
        boolean sameSnapshot) {
}
