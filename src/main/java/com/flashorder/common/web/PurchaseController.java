package com.flashorder.common.web;

import com.flashorder.order.OrderMapper;
import com.flashorder.order.OrderView;
import com.flashorder.request.PurchaseRequestService;
import com.flashorder.request.PurchaseResult;
import com.flashorder.request.Submission;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("api")
@ConditionalOnProperty(prefix = "flashorder", name = "intake-mode", havingValue = "sync")
public class PurchaseController {

    private final PurchaseRequestService requests;
    private final OrderMapper orders;
    private final MeterRegistry meters;

    public PurchaseController(PurchaseRequestService requests, OrderMapper orders, MeterRegistry meters) {
        this.requests = requests;
        this.orders = orders;
        this.meters = meters;
    }

    @PostMapping("/campaigns/{campaignId}/purchase-requests")
    public ResponseEntity<PurchaseResult> submit(
            @PathVariable long campaignId,
            @RequestHeader("X-User-Id") long userId,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {
        requireUser(userId);
        if (campaignId <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid campaign ID");
        }
        if (idempotencyKey.isEmpty()
                || idempotencyKey.length() > 128
                || !idempotencyKey.chars().allMatch(character -> character >= 33 && character <= 126)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid idempotency key");
        }

        Submission submission = requests.submit(campaignId, userId, idempotencyKey);
        PurchaseResult result = submission.result();
        meters.counter("flashorder.purchase.submissions",
                "status", result.status(),
                "reason", result.reason() == null ? "NONE" : result.reason(),
                "replayed", Boolean.toString(submission.replayed())).increment();

        ResponseEntity.BodyBuilder response = switch (result.status()) {
            case "ORDER_CREATED" -> ResponseEntity.created(URI.create("/orders/" + result.orderUid()));
            case "REJECTED" -> ResponseEntity.status(HttpStatus.CONFLICT);
            default -> throw new IllegalStateException("Unexpected synchronous purchase result: " + result.status());
        };
        if (submission.replayed()) {
            response.header("Idempotent-Replayed", "true");
        }
        return response.body(result);
    }

    @GetMapping("/purchase-requests/{requestUid}")
    public ResponseEntity<PurchaseResult> findRequest(
            @PathVariable String requestUid, @RequestHeader("X-User-Id") long userId) {
        requireUser(userId);
        return ResponseEntity.of(requests.find(userId, requestUid));
    }

    @GetMapping("/orders/{orderUid}")
    public ResponseEntity<OrderView> findOrder(
            @PathVariable String orderUid, @RequestHeader("X-User-Id") long userId) {
        requireUser(userId);
        return ResponseEntity.ofNullable(orders.findOwnedOrder(userId, orderUid));
    }

    private static void requireUser(long userId) {
        if (userId <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid user ID");
        }
    }
}
