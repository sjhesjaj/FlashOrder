package com.flashorder.outbox;

import com.flashorder.outbox.OutboxRelayMapper.OutboxMessage;
import com.flashorder.outbox.OutboxRelayStore.ClaimedBatch;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future.State;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@Profile("scheduler")
@ConditionalOnProperty(name = "flashorder.intake-mode", havingValue = "async")
public class OutboxRelay {
    private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int MARK_RESERVE_MS = 1000;

    private final KafkaTemplate<String, String> kafka;
    private final OutboxRelayStore store;
    private final int batchSize;
    private final int leaseMs;
    private final long sendBudgetNanos;

    public OutboxRelay(KafkaTemplate<String, String> kafka, OutboxRelayStore store,
                       @Value("${flashorder.outbox-relay.batch-size:20}") int batchSize,
                       @Value("${flashorder.outbox-relay.lease-ms:30000}") int leaseMs) {
        Map<String, Object> producer = kafka.getProducerFactory().getConfigurationProperties();
        Map<String, Object> defaults = ProducerConfig.configDef().defaultValues();
        long maxBlockMs = producerMillis(producer, defaults, ProducerConfig.MAX_BLOCK_MS_CONFIG);
        long deliveryMs = producerMillis(producer, defaults, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        if (batchSize < 1 || leaseMs <= maxBlockMs + deliveryMs + MARK_RESERVE_MS) {
            throw new IllegalArgumentException("Outbox relay needs a positive batch size and a lease longer than "
                    + "Kafka max.block.ms plus delivery.timeout.ms and the 1000 ms database update reserve");
        }
        this.kafka = kafka;
        this.store = store;
        this.batchSize = batchSize;
        this.leaseMs = leaseMs;
        this.sendBudgetNanos = TimeUnit.MILLISECONDS.toNanos(maxBlockMs + deliveryMs);
    }

    @Transactional(propagation = Propagation.NEVER)
    public int publishBatch() {
        // Measuring before claim gives a conservative local deadline for the database lease.
        long ackDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(leaseMs - MARK_RESERVE_MS);
        ClaimedBatch batch = store.claimBatch(batchSize, leaseMs);
        if (batch.messages().isEmpty()) {
            return 0;
        }

        Map<Long, CompletableFuture<SendResult<String, String>>> sends = new LinkedHashMap<>();
        for (OutboxMessage message : batch.messages()) {
            if (ackDeadline - System.nanoTime() <= sendBudgetNanos) {
                break;
            }
            try {
                sends.put(message.id(), kafka.send(message.topic(), message.key(), message.payload()));
            } catch (RuntimeException failure) {
                LOG.debug("Could not enqueue outbox message {} for {}", message.id(), message.topic(), failure);
            }
        }

        List<Long> acknowledgedIds = awaitAcknowledgements(sends, ackDeadline);
        if (acknowledgedIds.size() < batch.messages().size()) {
            LOG.warn("{} of {} leased outbox messages have no confirmed Kafka delivery and can retry after lease expiry",
                    batch.messages().size() - acknowledgedIds.size(), batch.messages().size());
        }
        // The compare-and-set protects any rows that another relay reclaimed while this worker was delayed.
        return acknowledgedIds.isEmpty() ? 0 : store.markPublished(batch.leaseToken(), acknowledgedIds);
    }

    private static List<Long> awaitAcknowledgements(Map<Long, CompletableFuture<SendResult<String, String>>> sends,
                                                     long deadline) {
        CompletableFuture<?>[] futures = sends.values().toArray(CompletableFuture<?>[]::new);
        try {
            CompletableFuture.allOf(futures).get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (ExecutionException | TimeoutException failure) {
            LOG.debug("Outbox batch had an unsuccessful Kafka delivery", failure);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return List.of();
        }
        return sends.entrySet().stream()
                .filter(send -> send.getValue().state() == State.SUCCESS)
                .map(Map.Entry::getKey)
                .toList();
    }

    private static long producerMillis(Map<String, Object> producer, Map<String, Object> defaults, String name) {
        return Long.parseLong(producer.getOrDefault(name, defaults.get(name)).toString());
    }
}
