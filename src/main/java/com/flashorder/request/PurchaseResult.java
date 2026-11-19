package com.flashorder.request;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record PurchaseResult(String requestUid, String status, String reason, String orderUid) {
}
