package com.flashorder.order;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("scheduler")
public class OrderExpiryScheduler {
    private final OrderExpiryService expiry;

    public OrderExpiryScheduler(OrderExpiryService expiry) {
        this.expiry = expiry;
    }

    @Scheduled(fixedDelayString = "${flashorder.order-expiry.fixed-delay-ms:1000}")
    public void cancelExpiredOrders() {
        int cancelled;
        do {
            cancelled = expiry.cancelExpiredBatch();
        } while (cancelled > 0);
    }
}
