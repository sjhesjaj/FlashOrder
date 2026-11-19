package com.flashorder.outbox;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface OutboxMapper {
    default int insertOrderCreated(String eventId, String orderUid, String requestUid, long campaignId, long userId) {
        return insertOrderEvent(eventId, orderUid, requestUid, campaignId, userId, "ORDER_CREATED");
    }

    int insertOrderEvent(@Param("eventId") String eventId,
                         @Param("orderUid") String orderUid,
                         @Param("requestUid") String requestUid,
                         @Param("campaignId") long campaignId,
                         @Param("userId") long userId,
                         @Param("eventType") String eventType);
}
