package com.flashorder.request;

import com.flashorder.common.GeneratedKey;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface PurchaseRequestMapper {
    StoredPurchaseRequest findByKey(@Param("userId") long userId,
                                    @Param("idempotencyKey") String idempotencyKey);

    StoredPurchaseRequest findOwned(@Param("userId") long userId, @Param("requestUid") String requestUid);

    int insertIfCampaignOpen(@Param("campaignId") long campaignId,
                             @Param("userId") long userId,
                             @Param("idempotencyKey") String idempotencyKey,
                             @Param("requestUid") String requestUid,
                             @Param("generatedKey") GeneratedKey generatedKey);

    boolean campaignExists(@Param("campaignId") long campaignId);

    int insertOrderCommand(@Param("eventId") String eventId, @Param("requestId") long requestId,
                           @Param("requestUid") String requestUid, @Param("campaignId") long campaignId);

    int markCreated(@Param("requestId") long requestId, @Param("orderId") long orderId);

    int markRejected(@Param("requestId") long requestId, @Param("reason") String reason);

    record StoredPurchaseRequest(long campaignId, String requestUid, String status, String reason, String orderUid) {
        PurchaseResult result() {
            String publicReason = "SYSTEM_FAILED".equals(status) ? "REQUEST_FAILED" : reason;
            return new PurchaseResult(requestUid, status, publicReason, orderUid);
        }
    }
}
