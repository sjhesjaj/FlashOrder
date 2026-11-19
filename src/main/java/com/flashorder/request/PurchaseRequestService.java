package com.flashorder.request;

import com.flashorder.common.GeneratedKey;
import com.flashorder.common.UuidV7;
import com.flashorder.request.PurchaseRequestMapper.StoredPurchaseRequest;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PurchaseRequestService {
    private final PurchaseRequestMapper requests;
    private final PurchaseOrderProcessor orders;
    private final boolean asyncIntake;

    public PurchaseRequestService(PurchaseRequestMapper requests, PurchaseOrderProcessor orders,
                                  @Value("${flashorder.intake-mode:sync}") String intakeMode) {
        this.requests = requests;
        this.orders = orders;
        this.asyncIntake = switch (intakeMode) {
            case "sync" -> false;
            case "async" -> true;
            default -> throw new IllegalArgumentException("Invalid flashorder.intake-mode: " + intakeMode);
        };
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

        long requestId = requestKey.requireId();
        if (asyncIntake) {
            if (requests.insertOrderCommand(UuidV7.next(), requestId, requestUid, campaignId) != 1) {
                throw new IllegalStateException("Could not write the order command for " + requestUid);
            }
            return new Submission(new PurchaseResult(requestUid, "ACCEPTED", null, null), false);
        }

        return new Submission(orders.attemptOrder(requestId, requestUid, campaignId, userId), false);
    }

    public Optional<PurchaseResult> find(long userId, String requestUid) {
        return Optional.ofNullable(requests.findOwned(userId, requestUid))
                .map(StoredPurchaseRequest::result);
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
