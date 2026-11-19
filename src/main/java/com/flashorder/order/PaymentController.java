package com.flashorder.order;

import com.flashorder.order.PaymentService.PaymentSubmission;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("api")
public class PaymentController {
    private final PaymentService payments;
    private final MeterRegistry meters;

    public PaymentController(PaymentService payments, MeterRegistry meters) {
        this.payments = payments;
        this.meters = meters;
    }

    @PostMapping("/orders/{orderUid}/payments")
    public ResponseEntity<PaymentResult> pay(@PathVariable String orderUid,
                                              @RequestHeader("X-User-Id") long userId,
                                              @RequestHeader("Idempotency-Key") String idempotencyKey) {
        if (userId <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid user ID");
        }
        if (idempotencyKey.isEmpty()
                || idempotencyKey.length() > 128
                || !idempotencyKey.chars().allMatch(character -> character >= 33 && character <= 126)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid idempotency key");
        }

        return payments.pay(userId, orderUid, idempotencyKey)
                .map(this::response)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private ResponseEntity<PaymentResult> response(PaymentSubmission submission) {
        meters.counter("flashorder.payments", "result", submission.outcome().name(),
                "replayed", Boolean.toString(submission.replayed())).increment();
        PaymentResult result = submission.result();
        ResponseEntity.BodyBuilder response = "PAID".equals(result.status())
                ? ResponseEntity.ok()
                : ResponseEntity.status(HttpStatus.CONFLICT);
        if (submission.replayed()) {
            response.header("Idempotent-Replayed", "true");
        }
        return response.body(result);
    }
}
