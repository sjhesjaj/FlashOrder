package com.flashorder.order;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("scheduler")
public class OrderExpiryScheduler {
    private final OrderExpiryService expiry;
    private final Counter failures;

    public OrderExpiryScheduler(OrderExpiryService expiry, MeterRegistry registry) {
        this.expiry = expiry;
        this.failures = registry.counter("flashorder.order.expiry.failures");
    }

    @Scheduled(fixedDelayString = "${flashorder.order-expiry.fixed-delay-ms:1000}")
    public void cancelExpiredOrders() {
        try {
            int cancelled;
            do {
                cancelled = expiry.cancelExpiredBatch();
            } while (cancelled > 0);
        } catch (RuntimeException failure) {
            failures.increment();
            throw failure;
        }
    }
}
