package com.flashorder.request;

import com.flashorder.request.guard.CampaignProtectionSnapshot;
import com.flashorder.request.guard.PurchaseRateLimiter;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PurchaseProtectionTest {
    private static final long CAMPAIGN = 1;
    private static final long USER = 2;
    private static final String KEY = "test-key";
    private final PurchaseRateLimiter limiter = mock(PurchaseRateLimiter.class);
    private final CampaignProtectionSnapshot snapshot = mock(CampaignProtectionSnapshot.class);
    private final PurchaseRequestService requests = mock(PurchaseRequestService.class);
    private final PurchaseProtection protection = new PurchaseProtection(limiter, snapshot, requests);

    @Test
    void userQuotaRunsBeforeReplay() {
        var submission = protection.submit(CAMPAIGN, USER, KEY);

        assertThat(submission.result().reason()).isEqualTo("USER_RATE_LIMITED");
        verify(limiter).allowUser(USER);
        verifyNoInteractions(requests, snapshot);
    }

    @Test
    void replayDoesNotSpendCampaignQuotaOrReadTheSnapshots() {
        when(limiter.allowUser(USER)).thenReturn(true);
        var original = new Submission(new PurchaseResult("request", "ORDER_CREATED", null, "order"), true);
        when(requests.findReplay(CAMPAIGN, USER, KEY)).thenReturn(Optional.of(original));

        assertThat(protection.submit(CAMPAIGN, USER, KEY)).isEqualTo(original);
        verify(limiter, never()).allowCampaign(CAMPAIGN);
        verifyNoInteractions(snapshot);
    }

    @Test
    void campaignQuotaRunsBeforeTheAsyncBacklogCheck() {
        when(limiter.allowUser(USER)).thenReturn(true);
        when(limiter.allowCampaign(CAMPAIGN)).thenReturn(true);
        when(requests.usesAsyncIntake()).thenReturn(true);
        when(snapshot.backlogFull(CAMPAIGN)).thenReturn(true);

        assertThat(protection.submit(CAMPAIGN, USER, KEY).result().reason()).isEqualTo("IN_FLIGHT_FULL");
        var gates = inOrder(limiter, requests, snapshot);
        gates.verify(limiter).allowUser(USER);
        gates.verify(requests).findReplay(CAMPAIGN, USER, KEY);
        gates.verify(snapshot).fastRejectReason(CAMPAIGN);
        gates.verify(limiter).allowCampaign(CAMPAIGN);
        gates.verify(snapshot).backlogFull(CAMPAIGN);
        verify(requests, never()).submitAfterReplayCheck(CAMPAIGN, USER, KEY);
    }

    @Test
    void newStockRejectionRechecksForAConcurrentOriginalRequest() {
        when(limiter.allowUser(USER)).thenReturn(true);
        when(snapshot.fastRejectReason(CAMPAIGN)).thenReturn("SOLD_OUT");
        var original = new Submission(new PurchaseResult("request", "ORDER_CREATED", null, "order"), true);
        when(requests.findReplay(CAMPAIGN, USER, KEY)).thenReturn(Optional.empty(), Optional.of(original));

        assertThat(protection.submit(CAMPAIGN, USER, KEY)).isEqualTo(original);
        verify(limiter, never()).allowCampaign(CAMPAIGN);
    }
}
