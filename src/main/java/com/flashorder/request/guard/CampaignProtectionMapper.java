package com.flashorder.request.guard;

import java.time.LocalDateTime;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface CampaignProtectionMapper {
    List<CampaignState> readCampaigns();

    List<AcceptedCount> readAcceptedCounts();

    long countAccepted(@Param("campaignId") long campaignId);

    record CampaignState(long campaignId, LocalDateTime startsAt, LocalDateTime endsAt, Integer available,
                         LocalDateTime databaseTime) {
    }

    record AcceptedCount(long campaignId, long accepted) {
    }
}
