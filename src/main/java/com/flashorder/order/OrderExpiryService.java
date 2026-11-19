package com.flashorder.order;

import com.flashorder.common.UuidV7;
import com.flashorder.order.OrderExpiryMapper.ExpiredOrder;
import com.flashorder.outbox.OutboxMapper;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class OrderExpiryService {
    private final OrderExpiryMapper expiry;
    private final OutboxMapper outbox;
    private final int batchSize;
    private final Counter cancelledOrders;

    public OrderExpiryService(OrderExpiryMapper expiry, OutboxMapper outbox, MeterRegistry registry,
                              @Value("${flashorder.order-expiry.batch-size:200}") int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("flashorder.order-expiry.batch-size must be positive");
        }
        this.expiry = expiry;
        this.outbox = outbox;
        this.batchSize = batchSize;
        this.cancelledOrders = registry.counter("flashorder.orders.cancelled");
    }

    @Transactional
    public int cancelExpiredBatch() {
        List<ExpiredOrder> orders = expiry.lockExpiredBatch(batchSize);
        if (orders.isEmpty()) {
            return 0;
        }

        List<Long> orderIds = orders.stream().map(ExpiredOrder::id).toList();
        if (expiry.cancelOrders(orderIds) != orders.size()) {
            throw new IllegalStateException("Could not cancel every locked expired order");
        }

        // TreeMap keeps inventory locks in campaign ID order across scheduler instances.
        Map<Long, Integer> stockByCampaign = new TreeMap<>();
        for (ExpiredOrder order : orders) {
            if (outbox.insertOrderEvent(UuidV7.next(), order.orderUid(), order.requestUid(),
                    order.campaignId(), order.userId(), "ORDER_CANCELLED") != 1) {
                throw new IllegalStateException("Could not write cancellation event for " + order.orderUid());
            }
            stockByCampaign.merge(order.campaignId(), 1, Integer::sum);
        }

        for (Map.Entry<Long, Integer> restore : stockByCampaign.entrySet()) {
            if (expiry.restoreStock(restore.getKey(), restore.getValue()) != 1) {
                throw new IllegalStateException("Could not restore stock for campaign " + restore.getKey());
            }
        }
        int cancelledCount = orders.size();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                cancelledOrders.increment(cancelledCount);
            }
        });
        return cancelledCount;
    }
}
