package com.flashorder.request;

import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.util.Optional;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
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
public class PurchaseRequestController {

    private static final String RETRY_AFTER_SECONDS = "1";
    private final PurchaseRequestService requests;
    private final MeterRegistry meters;
    private final Optional<PurchaseProtection> protection;

    public PurchaseRequestController(PurchaseRequestService requests, MeterRegistry meters,
                                     Optional<PurchaseProtection> protection) {
        this.requests = requests;
        this.meters = meters;
        this.protection = protection;
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

        Submission submission = protection
                .map(guard -> guard.submit(campaignId, userId, idempotencyKey))
                .orElseGet(() -> requests.submit(campaignId, userId, idempotencyKey));
        PurchaseResult result = submission.result();
        meters.counter("flashorder.purchase.submissions",
                "status", result.status(),
                "reason", result.reason() == null ? "NONE" : result.reason(),
                "replayed", Boolean.toString(submission.replayed())).increment();

        ResponseEntity.BodyBuilder response = switch (result.status()) {
            case "ACCEPTED" -> ResponseEntity.accepted()
                    .location(URI.create("/purchase-requests/" + result.requestUid()))
                    .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS);
            case "ORDER_CREATED" -> ResponseEntity.created(URI.create("/orders/" + result.orderUid()));
            case "REJECTED", "SYSTEM_FAILED" -> ResponseEntity.status(HttpStatus.CONFLICT);
            case "NOT_ACCEPTED" -> notAccepted(result.reason());
            default -> throw new IllegalStateException("Unexpected purchase result: " + result.status());
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
        return requests.find(userId, requestUid).map(result -> {
            ResponseEntity.BodyBuilder response = ResponseEntity.ok();
            if ("ACCEPTED".equals(result.status())) {
                response.header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS);
            }
            return response.body(result);
        }).orElseGet(() -> ResponseEntity.notFound().build());
    }

    private static void requireUser(long userId) {
        if (userId <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid user ID");
        }
    }

    private static ResponseEntity.BodyBuilder notAccepted(String reason) {
        return switch (reason) {
            case "CAMPAIGN_NOT_FOUND" -> ResponseEntity.status(HttpStatus.NOT_FOUND);
            case "USER_RATE_LIMITED", "CAMPAIGN_RATE_LIMITED" -> ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS);
            case "IN_FLIGHT_FULL" -> ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS);
            case "SOLD_OUT" -> ResponseEntity.status(HttpStatus.CONFLICT)
                    .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS);
            default -> ResponseEntity.status(HttpStatus.CONFLICT);
        };
    }
}
