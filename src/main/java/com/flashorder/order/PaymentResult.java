package com.flashorder.order;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record PaymentResult(String orderUid, String status, Instant paidAt, String reason) {
}
