package com.flashorder.order;

import java.time.LocalDateTime;

public record OrderView(String orderUid, String status, LocalDateTime expireAt) {
}
