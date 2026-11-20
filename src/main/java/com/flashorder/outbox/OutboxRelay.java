package com.flashorder.outbox;

import com.flashorder.outbox.OutboxRelayMapper.OutboxMessage;
import com.flashorder.outbox.OutboxRelayStore.ClaimedBatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@Profile("scheduler")
@ConditionalOnProperty(name = "flashorder.intake-mode", havingValue = "async")
public class OutboxRelay {
    static final String BATCH_SIZE_PROPERTY = "${flashorder.outbox-relay.batch-size:100}";
    private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int MARK_RESERVE_MS = 1000;

    private final KafkaTemplate<String, String> kafka;
    private final OutboxRelayStore store;
    private final int batchSize;
    private final int leaseMs;
    private final int retryDelayMs;
    private final long sendBudgetNanos;
    private final Counter claimedMessages;
    private final Counter publishedMessages;
    private final Counter retriedMessages;
    private final Counter unresolvedMessages;
    private final AtomicBoolean deliveryDegraded = new AtomicBoolean();

    public OutboxRelay(KafkaTemplate<String, String> kafka, OutboxRelayStore store, MeterRegistry registry,
                       @Value(BATCH_SIZE_PROPERTY) int batchSize,
                       @Value("${flashorder.outbox-relay.lease-ms:30000}") int leaseMs,
                       @Value("${flashorder.outbox-relay.retry-delay-ms:1000}") int retryDelayMs) {
        Map<String, Object> producer = kafka.getProducerFactory().getConfigurationProperties();
        Map<String, Object> defaults = ProducerConfig.configDef().defaultValues();
        long maxBlockMs = producerMillis(producer, defaults, ProducerConfig.MAX_BLOCK_MS_CONFIG);
        long deliveryMs = producerMillis(producer, defaults, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        if (batchSize < 1 || leaseMs <= maxBlockMs + deliveryMs + MARK_RESERVE_MS) {
            throw new IllegalArgumentException("Outbox relay needs a positive batch size and a lease longer than "
                    + "Kafka max.block.ms plus delivery.timeout.ms and the 1000 ms database update reserve");
        }
        if (retryDelayMs < 1) {
            throw new IllegalArgumentException("flashorder.outbox-relay.retry-delay-ms must be positive");
        }
        this.kafka = kafka;
        this.store = store;
        this.batchSize = batchSize;
        this.leaseMs = leaseMs;
        this.retryDelayMs = retryDelayMs;
        this.sendBudgetNanos = TimeUnit.MILLISECONDS.toNanos(maxBlockMs + deliveryMs);
        this.claimedMessages = registry.counter("flashorder.outbox.relay.claimed");
        this.publishedMessages = registry.counter("flashorder.outbox.relay.published");
        this.retriedMessages = registry.counter("flashorder.outbox.relay.retry.scheduled");
        this.unresolvedMessages = registry.counter("flashorder.outbox.relay.unresolved");
    }

    @Transactional(propagation = Propagation.NEVER)
    public BatchResult publishBatch() {
        // Measuring before claim gives a conservative local deadline for the database lease.
        long ackDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(leaseMs - MARK_RESERVE_MS);
        ClaimedBatch batch = store.claimBatch(batchSize, leaseMs);
        if (batch.messages().isEmpty()) {
            return new BatchResult(0, 0);
        }
        claimedMessages.increment(batch.messages().size());

        Map<Long, CompletableFuture<SendResult<String, String>>> sends = new HashMap<>();
        for (OutboxMessage message : batch.messages()) {
            if (Thread.currentThread().isInterrupted() || ackDeadline - System.nanoTime() <= sendBudgetNanos) {
                break;
            }
            try {
                sends.put(message.id(), kafka.send(message.topic(), message.key(), message.payload()));
            } catch (RuntimeException failure) {
                LOG.debug("Could not enqueue outbox message {} for {}", message.id(), message.topic(), failure);
                if (NestedExceptionUtils.getMostSpecificCause(failure)
                        instanceof org.apache.kafka.common.errors.TimeoutException) {
                    break;
                }
            }
        }

        awaitAcknowledgements(sends, ackDeadline);
        if (Thread.currentThread().isInterrupted()) {
            unresolvedMessages.increment(batch.messages().size());
            return new BatchResult(batch.messages().size(), 0);
        }

        List<Long> acknowledgedIds = new ArrayList<>();
        List<Long> retryIds = new ArrayList<>();
        for (OutboxMessage message : batch.messages()) {
            var send = sends.get(message.id());
            if (send == null) {
                retryIds.add(message.id());
                continue;
            }
            switch (send.state()) {
                case SUCCESS -> acknowledgedIds.add(message.id());
                case FAILED -> retryIds.add(message.id());
                case RUNNING, CANCELLED -> {
                    // A broker acknowledgement may still arrive, so leave the lease in place.
                }
            }
        }

        // The compare-and-set protects any rows that another relay reclaimed while this worker was delayed.
        int published = acknowledgedIds.isEmpty() ? 0 : store.markPublished(batch.leaseToken(), acknowledgedIds);
        publishedMessages.increment(published);
        int retrying = retryIds.isEmpty() ? 0 : store.retrySoon(batch.leaseToken(), retryIds, retryDelayMs);
        retriedMessages.increment(retrying);
        int unresolved = batch.messages().size() - acknowledgedIds.size() - retryIds.size();
        unresolvedMessages.increment(unresolved);
        if (!retryIds.isEmpty() || unresolved > 0) {
            if (deliveryDegraded.compareAndSet(false, true)) {
                LOG.warn("Outbox delivery degraded. Shortened {} leases. {} sends remain unresolved",
                        retrying, unresolved);
            } else {
                LOG.debug("Outbox delivery still degraded. Shortened {} leases. {} sends remain unresolved",
                        retrying, unresolved);
            }
        } else if (deliveryDegraded.compareAndSet(true, false)) {
            LOG.info("Outbox delivery recovered");
        }
        return new BatchResult(batch.messages().size(), published);
    }

    private static void awaitAcknowledgements(Map<Long, CompletableFuture<SendResult<String, String>>> sends,
                                              long deadline) {
        CompletableFuture<?>[] futures = sends.values().toArray(CompletableFuture<?>[]::new);
        try {
            CompletableFuture.allOf(futures).get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (ExecutionException | TimeoutException failure) {
            LOG.debug("Outbox batch had an unsuccessful Kafka delivery", failure);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static long producerMillis(Map<String, Object> producer, Map<String, Object> defaults, String name) {
        return Long.parseLong(producer.getOrDefault(name, defaults.get(name)).toString());
    }

    public record BatchResult(int claimed, int published) {
    }
}
