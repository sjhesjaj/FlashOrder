package com.flashorder.order;

import com.flashorder.common.UuidV7;
import com.flashorder.order.PaymentMapper.LockedOrder;
import com.flashorder.order.PaymentMapper.PaidOrderDetails;
import com.flashorder.outbox.OutboxMapper;
import java.time.LocalDateTime;
import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {
    private final PaymentMapper payments;
    private final OutboxMapper outbox;

    public PaymentService(PaymentMapper payments, OutboxMapper outbox) {
        this.payments = payments;
        this.outbox = outbox;
    }

    @Transactional
    public Optional<PaymentSubmission> pay(long userId, String orderUid, String idempotencyKey) {
        LockedOrder order = payments.lockOwnedOrder(userId, orderUid);
        if (order == null) {
            return Optional.empty();
        }

        PaymentOutcome earlier = payments.findAttempt(order.id(), idempotencyKey);
        if (earlier != null) {
            return Optional.of(new PaymentSubmission(earlier.result(order.orderUid(), order.paidAt()), earlier, true));
        }

        PaymentOutcome outcome = switch (order.status()) {
            case "PAID" -> PaymentOutcome.ALREADY_PAID;
            case "CANCELLED" -> PaymentOutcome.REJECTED_CANCELLED;
            // NOW(6) belongs to this update, which starts after the order row is locked.
            case "PENDING" -> payments.markPaidBeforeDeadline(order.id()) == 1
                    ? PaymentOutcome.SUCCEEDED : PaymentOutcome.REJECTED_EXPIRED;
            default -> throw new IllegalStateException("Unexpected payment order state: " + order.status());
        };
        payments.insertAttempt(order.id(), idempotencyKey, outcome);

        LocalDateTime paidAt = order.paidAt();
        if (outcome == PaymentOutcome.SUCCEEDED) {
            PaidOrderDetails details = payments.findPaidOrderDetails(order.id());
            if (details == null) {
                throw new IllegalStateException("Missing purchase request for order " + order.orderUid());
            }
            paidAt = details.paidAt();
            outbox.insertOrderEvent(UuidV7.next(), order.orderUid(), details.requestUid(),
                    order.campaignId(), userId, "ORDER_PAID");
        }
        return Optional.of(new PaymentSubmission(outcome.result(order.orderUid(), paidAt), outcome, false));
    }

    public record PaymentSubmission(PaymentResult result, PaymentOutcome outcome, boolean replayed) {
    }
}
