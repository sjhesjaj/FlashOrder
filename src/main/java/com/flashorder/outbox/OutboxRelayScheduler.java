package com.flashorder.outbox;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("scheduler")
@ConditionalOnProperty(name = "flashorder.intake-mode", havingValue = "async")
@ConditionalOnProperty(name = "flashorder.outbox-relay.scheduling-enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelayScheduler {
    private static final int CLEANUP_BATCH_SIZE = 500;

    private final OutboxRelay relay;
    private final OutboxRelayStore store;

    public OutboxRelayScheduler(OutboxRelay relay, OutboxRelayStore store) {
        this.relay = relay;
        this.store = store;
    }

    @Scheduled(fixedDelayString = "${flashorder.outbox-relay.fixed-delay-ms:250}")
    public void publishDueMessages() {
        relay.publishBatch();
    }

    @Scheduled(cron = "${flashorder.outbox-relay.cleanup-cron:0 0 3 * * *}", zone = "UTC")
    public void cleanupPublishedMessages() {
        int deleted;
        do {
            deleted = store.deletePublishedBatch(CLEANUP_BATCH_SIZE);
        } while (deleted == CLEANUP_BATCH_SIZE);
    }
}
