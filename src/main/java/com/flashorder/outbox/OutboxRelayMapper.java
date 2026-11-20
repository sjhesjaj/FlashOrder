package com.flashorder.outbox;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface OutboxRelayMapper {
    List<OutboxMessage> lockDueMessages(@Param("batchSize") int batchSize);

    int leaseMessages(@Param("leaseToken") String leaseToken,
                      @Param("leaseMs") int leaseMs,
                      @Param("messageIds") List<Long> messageIds);

    int markPublished(@Param("leaseToken") String leaseToken, @Param("messageIds") List<Long> messageIds);

    int retrySoon(@Param("leaseToken") String leaseToken, @Param("messageIds") List<Long> messageIds,
                  @Param("retryDelayMs") int retryDelayMs);

    int deletePublishedBatch(@Param("retentionDays") int retentionDays, @Param("batchSize") int batchSize);

    record OutboxMessage(long id, String topic, String key, String payload) {
    }
}
