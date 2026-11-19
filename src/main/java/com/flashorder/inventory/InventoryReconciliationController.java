package com.flashorder.inventory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("api")
public class InventoryReconciliationController {

    private final InventoryMapper inventory;
    private final MeterRegistry meters;
    private final ConcurrentMap<Long, AtomicLong> latestGaps = new ConcurrentHashMap<>();

    public InventoryReconciliationController(InventoryMapper inventory, MeterRegistry meters) {
        this.inventory = inventory;
        this.meters = meters;
    }

    @GetMapping("/admin/campaigns/{campaignId}/reconciliation")
    public ResponseEntity<InventoryReconciliation> getReconciliation(@PathVariable long campaignId) {
        if (campaignId <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid campaign ID");
        }
        var counts = inventory.findInventoryCounts(campaignId);
        if (counts == null) {
            return ResponseEntity.notFound().build();
        }
        // Each query is consistent on its own. They can observe different commits.
        var discrepancies = inventory.findRequestOrderDiscrepancies(campaignId);
        long gap = (long) counts.total() - counts.available() - counts.pending() - counts.paid();
        boolean consistent = gap == 0 && discrepancies.createdWithoutOrder() == 0
                && discrepancies.nonCreatedWithOrder() == 0;

        latestGaps.computeIfAbsent(campaignId, id -> meters.gauge("flashorder.reconciliation.gap",
                Tags.of("campaign", Long.toString(id)), new AtomicLong(gap))).set(gap);
        return ResponseEntity.ok(new InventoryReconciliation(
                campaignId, counts.total(), counts.available(), counts.pending(), counts.paid(), gap,
                discrepancies.createdWithoutOrder(), discrepancies.nonCreatedWithOrder(), consistent, false));
    }
}
