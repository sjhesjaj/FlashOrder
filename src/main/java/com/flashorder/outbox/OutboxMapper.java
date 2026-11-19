package com.flashorder.outbox;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface OutboxMapper {
    int insertOrderCreated(@Param("eventId") String eventId,
                           @Param("orderUid") String orderUid,
                           @Param("requestUid") String requestUid,
                           @Param("campaignId") long campaignId,
                           @Param("userId") long userId);
}
