package com.flashorder.request;

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
                             @Param("requestUid") String requestUid);

    long lockIdByUid(@Param("requestUid") String requestUid);

    int markCreated(@Param("requestId") long requestId);

    int markRejected(@Param("requestId") long requestId, @Param("reason") String reason);

    record StoredPurchaseRequest(long campaignId, String requestUid, String status, String reason, String orderUid) {
        PurchaseResult result() {
            return new PurchaseResult(requestUid, status, reason, orderUid);
        }
    }
}
