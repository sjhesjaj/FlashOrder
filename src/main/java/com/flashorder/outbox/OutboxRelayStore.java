package com.flashorder.outbox;

import com.flashorder.common.UuidV7;
import com.flashorder.outbox.OutboxRelayMapper.OutboxMessage;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxRelayStore {
    private final OutboxRelayMapper mapper;
    private final int retentionDays;

    public OutboxRelayStore(OutboxRelayMapper mapper,
                            @Value("${flashorder.outbox-relay.retention-days:7}") int retentionDays) {
        if (retentionDays < 1) {
            throw new IllegalArgumentException("flashorder.outbox-relay.retention-days must be positive");
        }
        this.mapper = mapper;
        this.retentionDays = retentionDays;
    }

    @Transactional
    public ClaimedBatch claimBatch(int batchSize, int leaseMs) {
        String leaseToken = UuidV7.next();
        List<OutboxMessage> messages = mapper.lockDueMessages(batchSize);
        if (!messages.isEmpty()) {
            List<Long> messageIds = messages.stream().map(OutboxMessage::id).toList();
            if (mapper.leaseMessages(leaseToken, leaseMs, messageIds) != messages.size()) {
                throw new IllegalStateException("Could not lease every locked outbox message");
            }
        }
        return new ClaimedBatch(leaseToken, messages);
    }

    @Transactional
    public int markPublished(String leaseToken, List<Long> messageIds) {
        return mapper.markPublished(leaseToken, messageIds);
    }

    @Transactional
    public int retrySoon(String leaseToken, List<Long> messageIds, int retryDelayMs) {
        return mapper.retrySoon(leaseToken, messageIds, retryDelayMs);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int deletePublishedBatch(int batchSize) {
        return mapper.deletePublishedBatch(retentionDays, batchSize);
    }

    public record ClaimedBatch(String leaseToken, List<OutboxMessage> messages) {
    }
}
