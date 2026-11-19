package com.flashorder.request;

import com.flashorder.common.GeneratedKey;
import com.flashorder.common.UuidV7;
import com.flashorder.inventory.InventoryMapper;
import com.flashorder.order.OrderMapper;
import com.flashorder.outbox.OutboxMapper;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

@Service
public class PurchaseOrderProcessor {
    private final PurchaseRequestMapper requests;
    private final OrderMapper orders;
    private final InventoryMapper inventory;
    private final OutboxMapper outbox;

    public PurchaseOrderProcessor(PurchaseRequestMapper requests, OrderMapper orders,
                                  InventoryMapper inventory, OutboxMapper outbox) {
        this.requests = requests;
        this.orders = orders;
        this.inventory = inventory;
        this.outbox = outbox;
    }

    public PurchaseResult attemptOrder(long requestId, String requestUid, long campaignId, long userId) {
        TransactionStatus transaction = TransactionAspectSupport.currentTransactionStatus();
        // The caller already holds the request row lock. Keep it if the order is rejected.
        Object savepoint = transaction.createSavepoint();
        String orderUid = UuidV7.next();
        GeneratedKey orderKey = new GeneratedKey();
        try {
            orders.insertPending(orderUid, requestId, campaignId, userId, orderKey);
        } catch (DuplicateKeyException exception) {
            // The failed insert retains the duplicate index lock during this verification.
            if (orders.hasActiveOrderConflict(campaignId, userId, requestId, orderUid)) {
                return rejectOrder(transaction, savepoint, requestId, requestUid, "ALREADY_HOLDING");
            }
            throw exception;
        }

        outbox.insertOrderCreated(UuidV7.next(), orderUid, requestUid, campaignId, userId);
        if (requests.markCreated(requestId, orderKey.requireId()) != 1) {
            throw new IllegalStateException("Could not finish purchase request " + requestUid);
        }
        if (inventory.reserve(campaignId) == 0) {
            return rejectOrder(transaction, savepoint, requestId, requestUid, "SOLD_OUT");
        }
        return new PurchaseResult(requestUid, "ORDER_CREATED", null, orderUid);
    }

    private PurchaseResult rejectOrder(TransactionStatus transaction, Object savepoint,
                                       long requestId, String requestUid, String reason) {
        transaction.rollbackToSavepoint(savepoint);
        if (requests.markRejected(requestId, reason) != 1) {
            throw new IllegalStateException("Could not reject purchase request " + requestUid);
        }
        return new PurchaseResult(requestUid, "REJECTED", reason, null);
    }
}
