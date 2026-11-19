package com.flashorder.order;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface OrderExpiryMapper {
    List<ExpiredOrder> lockExpiredBatch(@Param("batchSize") int batchSize);

    int cancelOrders(@Param("orderIds") List<Long> orderIds);

    int restoreStock(@Param("campaignId") long campaignId, @Param("quantity") int quantity);

    record ExpiredOrder(long id, String orderUid, String requestUid, long campaignId, long userId) {
    }
}
