package com.flashorder.request;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface OrderCommandMapper {
    LockedRequest lockRequest(@Param("requestId") long requestId);

    int markSystemFailed(@Param("requestId") long requestId);

    int insertDeadLetter(DeadLetter deadLetter);

    record LockedRequest(long id, String requestUid, long campaignId, long userId, String status) {
    }

    record DeadLetter(String eventId, String messageKey, Long requestId, String requestUid, Long campaignId,
                      String sourceTopic, int sourcePartition, long sourceOffset, String errorType,
                      String originalPayload, boolean payloadTruncated) {
    }
}
