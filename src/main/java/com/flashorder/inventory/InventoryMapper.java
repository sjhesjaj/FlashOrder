package com.flashorder.inventory;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface InventoryMapper {
    int reserve(@Param("campaignId") long campaignId);
}
