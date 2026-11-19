package com.flashorder.request;

import com.flashorder.request.OrderCommandMapper.DeadLetter;
import com.flashorder.request.OrderCommandMapper.LockedRequest;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
@Profile("consumer")
@ConditionalOnProperty(name = "flashorder.intake-mode", havingValue = "async")
public class OrderCommandProcessingService {

    private static final int MAX_DEAD_LETTER_PAYLOAD_LENGTH = 4_096;

    private final OrderCommandMapper commands;
    private final PurchaseOrderProcessor orders;
    private final JsonMapper json;

    public OrderCommandProcessingService(OrderCommandMapper commands, PurchaseOrderProcessor orders, JsonMapper json) {
        this.commands = commands;
        this.orders = orders;
        this.json = json;
    }

    @Transactional
    public void process(OrderCommand command) {
        LockedRequest request = lockMatchingRequest(command);
        if (request == null) {
            throw new OrderCommand.Invalid("Order command does not match a stored purchase request");
        }
        if ("ACCEPTED".equals(request.status())) {
            orders.attemptOrder(request.id(), request.requestUid(), request.campaignId(), request.userId());
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recover(ConsumerRecord<?, ?> record, Exception failure) {
        LockedRequest request = lockRequestForRecovery(record);
        if (request != null) {
            if (!"ACCEPTED".equals(request.status())) {
                return;
            }
            if (commands.markSystemFailed(request.id()) != 1) {
                throw new IllegalStateException("Could not record failure for " + request.requestUid());
            }
        }
        commands.insertDeadLetter(deadLetter(record, failure, request));
    }

    private LockedRequest lockRequestForRecovery(ConsumerRecord<?, ?> record) {
        try {
            return lockMatchingRequest(OrderCommand.read(json, record));
        } catch (OrderCommand.Invalid ignored) {
            return null;
        }
    }

    private LockedRequest lockMatchingRequest(OrderCommand command) {
        LockedRequest request = commands.lockRequest(command.requestId());
        if (request == null || request.campaignId() != command.campaignId()
                || !request.requestUid().equals(command.requestUid())) {
            return null;
        }
        return request;
    }

    private static DeadLetter deadLetter(ConsumerRecord<?, ?> record, Exception failure, LockedRequest request) {
        String identity = request == null
                ? "source:" + record.topic() + ":" + record.partition() + ":" + record.offset()
                : "request:" + request.id();
        String eventId = UUID.nameUUIDFromBytes(("order-command-dlt:" + identity)
                .getBytes(StandardCharsets.UTF_8)).toString();
        String rawPayload = record.value() instanceof String text ? text : null;
        boolean truncated = rawPayload != null && rawPayload.length() > MAX_DEAD_LETTER_PAYLOAD_LENGTH;
        String payload = truncated ? rawPayload.substring(0, MAX_DEAD_LETTER_PAYLOAD_LENGTH) : rawPayload;
        return new DeadLetter(eventId, request == null ? eventId : Long.toString(request.campaignId()),
                request == null ? null : request.id(), request == null ? null : request.requestUid(),
                request == null ? null : request.campaignId(), record.topic(), record.partition(), record.offset(),
                NestedExceptionUtils.getMostSpecificCause(failure).getClass().getSimpleName(), payload, truncated);
    }
}
