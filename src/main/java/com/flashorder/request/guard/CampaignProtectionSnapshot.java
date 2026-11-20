package com.flashorder.request.guard;

import com.flashorder.request.guard.CampaignProtectionMapper.AcceptedCount;
import com.flashorder.request.guard.CampaignProtectionMapper.CampaignState;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("api")
@ConditionalOnProperty(name = "flashorder.protection.enabled", havingValue = "true")
public class CampaignProtectionSnapshot {
    private static final Logger LOG = LoggerFactory.getLogger(CampaignProtectionSnapshot.class);

    private final CampaignProtectionMapper mapper;
    private final LongSupplier nanoTime;
    private final boolean async;
    private final long boundaryMarginNanos;
    private final long campaignMaxAgeNanos;
    private final long backlogMaxAgeNanos;
    private final long backlogLimit;
    private volatile CampaignSample campaigns;
    private volatile BacklogSample backlog = new BacklogSample(0, Map.of(), false);

    @Autowired
    public CampaignProtectionSnapshot(
            CampaignProtectionMapper mapper, MeterRegistry meters,
            @Value("${flashorder.intake-mode:sync}") String intakeMode,
            @Value("${flashorder.protection.campaign-refresh-ms:200}") long campaignRefreshMs,
            @Value("${flashorder.protection.campaign-max-age-ms:600}") long campaignMaxAgeMs,
            @Value("${flashorder.protection.backlog-max-age-ms:3000}") long backlogMaxAgeMs,
            @Value("${flashorder.protection.backlog-limit:500}") long backlogLimit) {
        this(mapper, meters, "async".equals(intakeMode), campaignRefreshMs, campaignMaxAgeMs,
                backlogMaxAgeMs, backlogLimit, System::nanoTime);
    }

    CampaignProtectionSnapshot(CampaignProtectionMapper mapper, MeterRegistry meters, boolean async,
                               long campaignRefreshMs, long campaignMaxAgeMs, long backlogMaxAgeMs,
                               long backlogLimit, LongSupplier nanoTime) {
        if (campaignRefreshMs <= 0 || campaignMaxAgeMs <= 0 || backlogMaxAgeMs <= 0 || backlogLimit <= 0) {
            throw new IllegalArgumentException("Campaign protection intervals and backlog limit must be positive");
        }
        this.mapper = mapper;
        this.nanoTime = nanoTime;
        this.async = async;
        this.boundaryMarginNanos = TimeUnit.MILLISECONDS.toNanos(campaignRefreshMs);
        this.campaignMaxAgeNanos = TimeUnit.MILLISECONDS.toNanos(campaignMaxAgeMs);
        this.backlogMaxAgeNanos = TimeUnit.MILLISECONDS.toNanos(backlogMaxAgeMs);
        this.backlogLimit = backlogLimit;
        if (async) {
            Gauge.builder("flashorder.requests.accepted", this, CampaignProtectionSnapshot::lastAcceptedCount)
                    .description("Accepted requests at the last successful backlog refresh")
                    .register(meters);
        }
    }

    @Scheduled(fixedDelayString = "${flashorder.protection.campaign-refresh-ms:200}")
    public void refreshCampaigns() {
        long started = nanoTime.getAsLong();
        try {
            var rows = mapper.readCampaigns();
            long finished = nanoTime.getAsLong();
            campaigns = new CampaignSample(started, finished, rows.stream()
                    .collect(Collectors.toUnmodifiableMap(CampaignState::campaignId, Function.identity())));
        } catch (DataAccessException failure) {
            LOG.debug("Campaign snapshot refresh failed", failure);
        }
    }

    public String fastRejectReason(long campaignId) {
        CampaignSample sample = campaigns;
        long now = nanoTime.getAsLong();
        if (sample == null || now - sample.startedNanos() > campaignMaxAgeNanos) {
            return null;
        }
        CampaignState state = sample.states().get(campaignId);
        if (state == null) {
            return null;
        }

        // The DB clock was sampled between the start and end of the read.
        // Leave one refresh interval around the boundaries for the database gate.
        LocalDateTime earliest = state.databaseTime().plusNanos(now - sample.finishedNanos())
                .minusNanos(boundaryMarginNanos);
        LocalDateTime latest = state.databaseTime().plusNanos(now - sample.startedNanos())
                .plusNanos(boundaryMarginNanos);
        if (latest.isBefore(state.startsAt()) || !earliest.isBefore(state.endsAt())) {
            return "CAMPAIGN_CLOSED";
        }
        if (!earliest.isBefore(state.startsAt()) && latest.isBefore(state.endsAt())
                && Integer.valueOf(0).equals(state.available())) {
            return "SOLD_OUT";
        }
        return null;
    }

    @Scheduled(fixedDelayString = "${flashorder.protection.backlog-refresh-ms:1000}")
    public void refreshBacklog() {
        if (!async) {
            return;
        }
        long started = nanoTime.getAsLong();
        try {
            Map<Long, Long> counts = mapper.readAcceptedCounts().stream()
                    .collect(Collectors.toUnmodifiableMap(AcceptedCount::campaignId, AcceptedCount::accepted));
            backlog = new BacklogSample(started, counts, true);
        } catch (DataAccessException failure) {
            BacklogSample previous = backlog;
            backlog = new BacklogSample(previous.startedNanos(), previous.counts(), false);
            LOG.debug("Backlog snapshot refresh failed", failure);
        }
    }

    public boolean backlogFull(long campaignId) {
        if (!async) {
            return false;
        }
        BacklogSample sample = backlog;
        if (sample.valid() && nanoTime.getAsLong() - sample.startedNanos() <= backlogMaxAgeNanos) {
            return sample.counts().getOrDefault(campaignId, 0L) >= backlogLimit;
        }
        return mapper.countAccepted(campaignId) >= backlogLimit;
    }

    private double lastAcceptedCount() {
        return backlog.counts().values().stream().mapToLong(Long::longValue).sum();
    }

    private record CampaignSample(long startedNanos, long finishedNanos, Map<Long, CampaignState> states) {
    }

    private record BacklogSample(long startedNanos, Map<Long, Long> counts, boolean valid) {
    }
}
