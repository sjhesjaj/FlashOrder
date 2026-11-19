package com.flashorder.order;

import com.flashorder.common.GeneratedKey;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface OrderMapper {
    int insertPending(@Param("orderUid") String orderUid,
                      @Param("requestId") long requestId,
                      @Param("campaignId") long campaignId,
                      @Param("userId") long userId,
                      @Param("generatedKey") GeneratedKey generatedKey);

    boolean hasActiveOrderConflict(@Param("campaignId") long campaignId,
                                   @Param("userId") long userId,
                                   @Param("requestId") long requestId,
                                   @Param("orderUid") String orderUid);

    OrderView findOwnedOrder(@Param("userId") long userId, @Param("orderUid") String orderUid);
}
