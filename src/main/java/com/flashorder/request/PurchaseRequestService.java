package com.flashorder.request;

import com.flashorder.common.UuidV7;
import com.flashorder.inventory.InventoryMapper;
import com.flashorder.order.OrderMapper;
import com.flashorder.outbox.OutboxMapper;
import com.flashorder.request.PurchaseRequestMapper.StoredPurchaseRequest;

import java.sql.SQLException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

@Service
public class PurchaseRequestService {
    private static final Pattern MYSQL_DUPLICATE_KEY =
            Pattern.compile("for key ['`](?:[^'`]+\\.)?([a-zA-Z0-9_]+)['`]\\s*$");

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

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Submission submit(long campaignId, long userId, String idempotencyKey) {
        StoredPurchaseRequest existing = requests.findByKey(userId, idempotencyKey);
        if (existing != null) {
            return replay(campaignId, existing);
        }

        String requestUid = UuidV7.next();
        try {
            if (requests.insertIfCampaignOpen(campaignId, userId, idempotencyKey, requestUid) == 0) {
                // A concurrent original submission may have committed as the campaign closed.
                existing = requests.findByKey(userId, idempotencyKey);
                return existing == null ? rejectUnaccepted("CAMPAIGN_CLOSED") : replay(campaignId, existing);
            }
        } catch (DuplicateKeyException exception) {
            if (isDuplicateKey(exception, "uk_purchase_request_idempotency")) {
                // The winning transaction has committed by the time the duplicate is reported.
                existing = requests.findByKey(userId, idempotencyKey);
                if (existing != null) {
                    return replay(campaignId, existing);
                }
            }
            throw exception;
        }

        long requestId = requests.lockIdByUid(requestUid);
        PurchaseResult result = attemptOrder(requestId, requestUid, campaignId, userId);
        return new Submission(result, false);
    }

    public Optional<PurchaseResult> find(long userId, String requestUid) {
        return Optional.ofNullable(requests.findOwned(userId, requestUid))
                .map(StoredPurchaseRequest::result);
    }

    private PurchaseResult attemptOrder(long requestId, String requestUid, long campaignId, long userId) {
        TransactionStatus transaction = TransactionAspectSupport.currentTransactionStatus();
        // Preserve the request and its uniqueness constraint while undoing a business rejection.
        Object savepoint = transaction.createSavepoint();
        String orderUid = UuidV7.next();
        try {
            orders.insertPending(orderUid, requestId, campaignId, userId);
        } catch (DuplicateKeyException exception) {
            if (isDuplicateKey(exception, "uk_purchase_order_active")) {
                return rejectOrder(transaction, savepoint, requestId, requestUid, "ALREADY_HOLDING");
            }
            throw exception;
        }

        outbox.insertOrderCreated(UuidV7.next(), orderUid, requestUid, campaignId, userId);
        if (requests.markCreated(requestId) != 1) {
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
        return new Submission(new PurchaseResult(null, "REJECTED", reason, null), false);
    }

    private static boolean isDuplicateKey(DuplicateKeyException exception, String key) {
        if (!(exception.getMostSpecificCause() instanceof SQLException sqlException)
                || sqlException.getErrorCode() != 1062) {
            return false;
        }
        Matcher matcher = MYSQL_DUPLICATE_KEY.matcher(sqlException.getMessage());
        return matcher.find() && key.equals(matcher.group(1));
    }
}
