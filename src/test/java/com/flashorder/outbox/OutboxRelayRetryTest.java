package com.flashorder.outbox;

import com.flashorder.outbox.OutboxRelay.BatchResult;
import com.flashorder.outbox.OutboxRelayMapper.OutboxMessage;
import com.flashorder.outbox.OutboxRelayStore.ClaimedBatch;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxRelayRetryTest {
    private static final String TOPIC = "flashorder.order-commands";
    private static final String LEASE = "lease-token";
    private static final int BATCH_SIZE = 5;
    private static final int LEASE_MS = 2000;
    private static final int RETRY_DELAY_MS = 100;

    @Mock
    private KafkaTemplate<String, String> kafka;
    @Mock
    private ProducerFactory<String, String> producerFactory;
    @Mock
    private OutboxRelayStore store;
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private OutboxRelay relay;

    @BeforeEach
    void configureRelay() {
        when(kafka.getProducerFactory()).thenReturn(producerFactory);
        when(producerFactory.getConfigurationProperties()).thenReturn(Map.of(
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 10, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10));
        relay = new OutboxRelay(kafka, store, meters, BATCH_SIZE, LEASE_MS, RETRY_DELAY_MS);
    }

    @Test
    void retriesFailedAndUnsentMessagesAndKeepsTheCancelledSendLeased() {
        when(store.claimBatch(BATCH_SIZE, LEASE_MS)).thenReturn(new ClaimedBatch(
                LEASE, List.of(message(1), message(2), message(3), message(4), message(5))));
        when(kafka.send(TOPIC, "1", "{}")).thenReturn(CompletableFuture.completedFuture(null));
        when(kafka.send(TOPIC, "2", "{}")).thenReturn(CompletableFuture.failedFuture(
                new TimeoutException("Producer delivery deadline reached")));
        var cancelled = new CompletableFuture<SendResult<String, String>>();
        cancelled.cancel(false);
        when(kafka.send(TOPIC, "3", "{}")).thenReturn(cancelled);
        when(kafka.send(TOPIC, "4", "{}")).thenThrow(new KafkaException(
                "Send failed", new TimeoutException("Broker metadata unavailable")));
        when(store.markPublished(LEASE, List.of(1L))).thenReturn(1);
        when(store.retrySoon(LEASE, List.of(2L, 4L, 5L), RETRY_DELAY_MS)).thenReturn(3);

        assertThat(relay.publishBatch()).isEqualTo(new BatchResult(5, 1));
        verify(store).markPublished(LEASE, List.of(1L));
        verify(store).retrySoon(LEASE, List.of(2L, 4L, 5L), RETRY_DELAY_MS);
        verify(kafka, never()).send(TOPIC, "5", "{}");
        assertThat(meters.get("flashorder.outbox.relay.claimed").counter().count()).isEqualTo(5);
        assertThat(meters.get("flashorder.outbox.relay.published").counter().count()).isEqualTo(1);
        assertThat(meters.get("flashorder.outbox.relay.retry.scheduled").counter().count()).isEqualTo(3);
        assertThat(meters.get("flashorder.outbox.relay.unresolved").counter().count()).isEqualTo(1);
    }

    @Test
    void keepsTheOriginalLeaseWhenTheRelayStopsWaitingForAnUnfinishedSend() {
        when(store.claimBatch(BATCH_SIZE, LEASE_MS))
                .thenReturn(new ClaimedBatch(LEASE, List.of(message(1))));
        var pending = new CompletableFuture<SendResult<String, String>>();
        when(kafka.send(TOPIC, "1", "{}")).thenReturn(pending);

        assertThat(relay.publishBatch()).isEqualTo(new BatchResult(1, 0));
        assertThat(pending).isNotDone();
        verify(store, never()).markPublished(anyString(), anyList());
        verify(store, never()).retrySoon(LEASE, List.of(1L), RETRY_DELAY_MS);
    }

    private static OutboxMessage message(long id) {
        return new OutboxMessage(id, TOPIC, Long.toString(id), "{}");
    }
}
