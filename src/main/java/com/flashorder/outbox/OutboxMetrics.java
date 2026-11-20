package com.flashorder.outbox;

import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("scheduler")
@ConditionalOnProperty(name = "flashorder.intake-mode", havingValue = "async")
public class OutboxMetrics {
    private final OutboxRelayMapper mapper;
    private final AtomicReference<Double> unpublished = new AtomicReference<>(Double.NaN);

    public OutboxMetrics(OutboxRelayMapper mapper, MeterRegistry registry) {
        this.mapper = mapper;
        Gauge.builder("flashorder.outbox.unpublished", unpublished, AtomicReference::get).register(registry);
    }

    @Scheduled(fixedDelayString = "${flashorder.outbox-relay.metrics-refresh-ms:1000}")
    public void refreshUnpublished() {
        try {
            unpublished.set((double) mapper.countUnpublished());
        } catch (DataAccessException failure) {
            unpublished.set(Double.NaN);
        }
    }
}
