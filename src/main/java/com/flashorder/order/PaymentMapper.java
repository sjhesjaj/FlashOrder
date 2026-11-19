package com.flashorder.order;

import java.time.LocalDateTime;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface PaymentMapper {
    LockedOrder lockOwnedOrder(@Param("userId") long userId, @Param("orderUid") String orderUid);

    PaymentOutcome findAttempt(@Param("orderId") long orderId, @Param("idempotencyKey") String idempotencyKey);

    int markPaidBeforeDeadline(@Param("orderId") long orderId);

    int insertAttempt(@Param("orderId") long orderId, @Param("idempotencyKey") String idempotencyKey,
                      @Param("result") PaymentOutcome result);

    PaidOrderDetails findPaidOrderDetails(@Param("orderId") long orderId);

    record LockedOrder(long id, String orderUid, long campaignId, String status, LocalDateTime paidAt) {
    }

    record PaidOrderDetails(String requestUid, LocalDateTime paidAt) {
    }
}
