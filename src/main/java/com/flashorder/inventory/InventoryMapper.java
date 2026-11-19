package com.flashorder.inventory;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface InventoryMapper {
    int reserve(@Param("campaignId") long campaignId);

    InventoryCounts findInventoryCounts(@Param("campaignId") long campaignId);

    RequestOrderDiscrepancies findRequestOrderDiscrepancies(@Param("campaignId") long campaignId);

    record InventoryCounts(int total, int available, long pending, long paid) {
    }

    record RequestOrderDiscrepancies(long createdWithoutOrder, long nonCreatedWithOrder) {
    }
}
