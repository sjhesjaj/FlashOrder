package com.flashorder.request;

import com.flashorder.common.GeneratedKey;
import com.flashorder.common.UuidV7;
import com.flashorder.inventory.InventoryMapper;
import com.flashorder.order.OrderMapper;
import com.flashorder.outbox.OutboxMapper;
import com.flashorder.request.PurchaseRequestMapper.StoredPurchaseRequest;

import java.util.Optional;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

@Service
public class PurchaseRequestService {
    private final PurchaseRequestMapper requests;
    private final OrderMapper orders;
    private final InventoryMapper inventory;
    private final OutboxMapper outbox;

    public PurchaseRequestService(PurchaseRequestMapper requests, OrderMapper orders,
                                  InventoryMapper inventory, OutboxMapper outbox) {
        this.requests = requests;
        this.orders = orders;
        this.inventory = inventory;
        this.outbox = outbox;
    }

    @Transactional
    public Submission submit(long campaignId, long userId, String idempotencyKey) {
        StoredPurchaseRequest existing = requests.findByKey(userId, idempotencyKey);
        if (existing != null) {
            return replay(campaignId, existing);
        }

        String requestUid = UuidV7.next();
        GeneratedKey requestKey = new GeneratedKey();
        int inserted;
        try {
            inserted = requests.insertIfCampaignOpen(campaignId, userId, idempotencyKey, requestUid, requestKey);
        } catch (DuplicateKeyException exception) {
            existing = requests.findByKey(userId, idempotencyKey);
            if (existing != null) {
                return replay(campaignId, existing);
            }
            throw exception;
        }

        if (inserted == 0) {
            boolean campaignExists = requests.campaignExists(campaignId);
            // A concurrent submission may have committed while the campaign was closing.
            existing = requests.findByKey(userId, idempotencyKey);
            if (existing != null) {
                return replay(campaignId, existing);
            }
            return rejectUnaccepted(campaignExists ? "CAMPAIGN_CLOSED" : "CAMPAIGN_NOT_FOUND");
        }

        PurchaseResult result = attemptOrder(requestKey.requireId(), requestUid, campaignId, userId);
        return new Submission(result, false);
    }

    public Optional<PurchaseResult> find(long userId, String requestUid) {
        return Optional.ofNullable(requests.findOwned(userId, requestUid))
                .map(StoredPurchaseRequest::result);
    }

    private PurchaseResult attemptOrder(long requestId, String requestUid, long campaignId, long userId) {
        TransactionStatus transaction = TransactionAspectSupport.currentTransactionStatus();
        // The insert already holds the request row lock. Keep it if the order is rejected.
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

    private static Submission replay(long campaignId, StoredPurchaseRequest existing) {
        if (existing.campaignId() != campaignId) {
            return rejectUnaccepted("IDEMPOTENCY_CONFLICT");
        }
        return new Submission(existing.result(), true);
    }

    private static Submission rejectUnaccepted(String reason) {
        return new Submission(new PurchaseResult(null, "NOT_ACCEPTED", reason, null), false);
    }
}
