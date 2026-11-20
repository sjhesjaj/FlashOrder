package com.flashorder.request.guard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.flashorder.request.guard.CampaignProtectionMapper.AcceptedCount;
import com.flashorder.request.guard.CampaignProtectionMapper.CampaignState;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.dao.TransientDataAccessResourceException;

class CampaignProtectionSnapshotTest {
    private static final LocalDateTime DB_NOW = LocalDateTime.of(2040, 1, 1, 12, 0);
    private final CampaignProtectionMapper mapper = mock(CampaignProtectionMapper.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final AtomicLong clock = new AtomicLong();
    private final CampaignProtectionSnapshot snapshot = protection(true);

    @Test
    void soldOutSampleExpiresAfterFailedRefreshAndNextRefreshSeesRestock() {
        when(mapper.readCampaigns())
                .thenReturn(List.of(activeCampaign(1, 0)))
                .thenThrow(new TransientDataAccessResourceException("snapshot read failed"))
                .thenReturn(List.of(activeCampaign(1, 2)));
        assertThat(snapshot.fastRejectReason(1)).isNull();

        snapshot.refreshCampaigns();
        assertThat(snapshot.fastRejectReason(1)).isEqualTo("SOLD_OUT");
        assertThat(snapshot.fastRejectReason(2)).isNull();
        advance(200);
        snapshot.refreshCampaigns();
        assertThat(snapshot.fastRejectReason(1)).isEqualTo("SOLD_OUT");
        advance(401);
        assertThat(snapshot.fastRejectReason(1)).isNull();

        snapshot.refreshCampaigns();
        assertThat(snapshot.fastRejectReason(1)).isNull();
    }

    @Test
    void usesDatabaseTimeAndLeavesWindowBoundariesToTransactionalCheck() {
        when(mapper.readCampaigns()).thenReturn(List.of(
                new CampaignState(1, DB_NOW.plusSeconds(2), DB_NOW.plusHours(1), null, DB_NOW),
                new CampaignState(2, DB_NOW.minusHours(1), DB_NOW.minusSeconds(2), null, DB_NOW),
                new CampaignState(3, DB_NOW.plusNanos(100_000_000), DB_NOW.plusHours(1), null, DB_NOW),
                new CampaignState(4, DB_NOW.minusHours(1), DB_NOW.plusNanos(100_000_000), 0, DB_NOW),
                activeCampaign(5, null)));

        snapshot.refreshCampaigns();

        assertThat(snapshot.fastRejectReason(1)).isEqualTo("CAMPAIGN_CLOSED");
        assertThat(snapshot.fastRejectReason(2)).isEqualTo("CAMPAIGN_CLOSED");
        assertThat(snapshot.fastRejectReason(3)).isNull();
        assertThat(snapshot.fastRejectReason(4)).isNull();
        assertThat(snapshot.fastRejectReason(5)).isNull();
    }

    @Test
    void slowQueryCannotPublishAnAlreadyExpiredCampaignSampleAsFresh() {
        when(mapper.readCampaigns()).thenAnswer(invocation -> {
            advance(601);
            return List.of(activeCampaign(1, 0));
        });

        snapshot.refreshCampaigns();

        assertThat(snapshot.fastRejectReason(1)).isNull();
    }

    @Test
    void usesBatchedBacklogCountsAndTreatsMissingCampaignAsZeroForFreshSample() {
        when(mapper.readAcceptedCounts()).thenReturn(List.of(new AcceptedCount(1, 500), new AcceptedCount(2, 499)));
        snapshot.refreshBacklog();

        assertThat(snapshot.backlogFull(1)).isTrue();
        assertThat(snapshot.backlogFull(2)).isFalse();
        assertThat(snapshot.backlogFull(3)).isFalse();
        verify(mapper, never()).countAccepted(anyLong());
        assertThat(meters.get("flashorder.requests.accepted").gauge().value()).isEqualTo(999.0);
    }

    @Test
    void usesTargetedCountWithNoSnapshotAfterFailureOrWhenTooOld() {
        when(mapper.countAccepted(1)).thenReturn(499L);
        when(mapper.readAcceptedCounts())
                .thenReturn(List.of(new AcceptedCount(1, 500)))
                .thenThrow(new TransientDataAccessResourceException("snapshot read failed"))
                .thenReturn(List.of(new AcceptedCount(1, 500)));
        assertThat(snapshot.backlogFull(1)).isFalse();

        snapshot.refreshBacklog();
        assertThat(snapshot.backlogFull(1)).isTrue();
        snapshot.refreshBacklog();
        assertThat(snapshot.backlogFull(1)).isFalse();
        assertThat(meters.get("flashorder.requests.accepted").gauge().value()).isEqualTo(500.0);

        snapshot.refreshBacklog();
        assertThat(snapshot.backlogFull(1)).isTrue();
        advance(3_001);
        assertThat(snapshot.backlogFull(1)).isFalse();
    }

    @Test
    void synchronousModeDoesNotReadBacklog() {
        CampaignProtectionSnapshot sync = protection(false);
        sync.refreshBacklog();

        assertThat(sync.backlogFull(1)).isFalse();
        verifyNoInteractions(mapper);
    }

    private CampaignProtectionSnapshot protection(boolean async) {
        return new CampaignProtectionSnapshot(mapper, meters, async, 200, 600, 3000, 500, clock::get);
    }

    private void advance(long milliseconds) {
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(milliseconds));
    }

    private static CampaignState activeCampaign(long campaignId, Integer available) {
        return new CampaignState(campaignId, DB_NOW.minusHours(1), DB_NOW.plusHours(1), available, DB_NOW);
    }
}
