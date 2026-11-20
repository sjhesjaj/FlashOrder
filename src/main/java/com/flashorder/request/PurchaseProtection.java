package com.flashorder.request;

import com.flashorder.request.guard.CampaignProtectionSnapshot;
import com.flashorder.request.guard.PurchaseRateLimiter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("api")
@ConditionalOnProperty(name = "flashorder.protection.enabled", havingValue = "true")
public class PurchaseProtection {
    private final PurchaseRateLimiter limiter;
    private final CampaignProtectionSnapshot snapshot;
    private final PurchaseRequestService requests;

    public PurchaseProtection(PurchaseRateLimiter limiter, CampaignProtectionSnapshot snapshot,
                              PurchaseRequestService requests) {
        this.limiter = limiter;
        this.snapshot = snapshot;
        this.requests = requests;
    }

    public Submission submit(long campaignId, long userId, String idempotencyKey) {
        if (!limiter.allowUser(userId)) {
            return PurchaseRequestService.rejectUnaccepted("USER_RATE_LIMITED");
        }
        var replay = requests.findReplay(campaignId, userId, idempotencyKey);
        if (replay.isPresent()) {
            return replay.get();
        }

        String reason = checkNewRequest(campaignId);
        if (reason != null) {
            // A concurrent original request can commit while this copy runs the guards.
            return requests.findReplay(campaignId, userId, idempotencyKey)
                    .orElseGet(() -> PurchaseRequestService.rejectUnaccepted(reason));
        }
        return requests.submitAfterReplayCheck(campaignId, userId, idempotencyKey);
    }

    private String checkNewRequest(long campaignId) {
        String reason = snapshot.fastRejectReason(campaignId);
        if (reason != null) {
            return reason;
        }
        if (!limiter.allowCampaign(campaignId)) {
            return "CAMPAIGN_RATE_LIMITED";
        }
        return requests.usesAsyncIntake() && snapshot.backlogFull(campaignId) ? "IN_FLIGHT_FULL" : null;
    }
}
