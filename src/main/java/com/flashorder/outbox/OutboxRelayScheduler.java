package com.flashorder.outbox;

import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
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
    private final int batchSize;
    private final long maxRoundNanos;

    public OutboxRelayScheduler(OutboxRelay relay, OutboxRelayStore store,
                                @Value(OutboxRelay.BATCH_SIZE_PROPERTY) int batchSize,
                                @Value("${flashorder.outbox-relay.max-round-ms:1000}") long maxRoundMs) {
        if (maxRoundMs < 1) {
            throw new IllegalArgumentException("flashorder.outbox-relay.max-round-ms must be positive");
        }
        this.relay = relay;
        this.store = store;
        this.batchSize = batchSize;
        this.maxRoundNanos = TimeUnit.MILLISECONDS.toNanos(maxRoundMs);
    }

    @Scheduled(fixedDelayString = "${flashorder.outbox-relay.fixed-delay-ms:250}")
    public void publishDueMessages() {
        long started = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
            OutboxRelay.BatchResult result = relay.publishBatch();
            if (result.claimed() != batchSize || result.published() != result.claimed()
                    || System.nanoTime() - started >= maxRoundNanos) {
                return;
            }
        }
    }

    @Scheduled(cron = "${flashorder.outbox-relay.cleanup-cron:0 0 3 * * *}", zone = "UTC")
    public void cleanupPublishedMessages() {
        int deleted;
        do {
            deleted = store.deletePublishedBatch(CLEANUP_BATCH_SIZE);
        } while (deleted == CLEANUP_BATCH_SIZE);
    }
}
