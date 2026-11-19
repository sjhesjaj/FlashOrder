package com.flashorder.order;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

public enum PaymentOutcome {
    SUCCEEDED("PAID", null),
    ALREADY_PAID("PAID", null),
    REJECTED_CANCELLED("REJECTED", "ORDER_CANCELLED"),
    REJECTED_EXPIRED("REJECTED", "ORDER_EXPIRED");

    private final String status;
    private final String reason;

    PaymentOutcome(String status, String reason) {
        this.status = status;
        this.reason = reason;
    }

    PaymentResult result(String orderUid, LocalDateTime paidAt) {
        Instant paidAtUtc = "PAID".equals(status)
                ? Objects.requireNonNull(paidAt, "Paid order is missing paid_at").toInstant(ZoneOffset.UTC)
                : null;
        return new PaymentResult(orderUid, status, paidAtUtc, reason);
    }
}
