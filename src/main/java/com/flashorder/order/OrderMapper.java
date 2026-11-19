package com.flashorder.order;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface OrderMapper {
    int insertPending(@Param("orderUid") String orderUid,
                      @Param("requestId") long requestId,
                      @Param("campaignId") long campaignId,
                      @Param("userId") long userId);

    OrderView findOwnedOrder(@Param("userId") long userId, @Param("orderUid") String orderUid);
}
