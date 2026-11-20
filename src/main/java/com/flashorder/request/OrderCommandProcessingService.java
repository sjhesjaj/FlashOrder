package com.flashorder.request;

import com.flashorder.request.OrderCommandMapper.DeadLetter;
import com.flashorder.request.OrderCommandMapper.LockedRequest;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;

@Service
@Profile("consumer")
@ConditionalOnProperty(name = "flashorder.intake-mode", havingValue = "async")
public class OrderCommandProcessingService {

    private static final int MAX_DEAD_LETTER_PAYLOAD_LENGTH = 4_096;

    private final OrderCommandMapper commands;
    private final PurchaseOrderProcessor orders;
    private final JsonMapper json;
    private final Map<String, Timer> terminalLatency;

    public OrderCommandProcessingService(OrderCommandMapper commands, PurchaseOrderProcessor orders, JsonMapper json,
                                         MeterRegistry meters) {
        this.commands = commands;
        this.orders = orders;
        this.json = json;
        this.terminalLatency = Map.of(
                "ORDER_CREATED", terminalTimer(meters, "ORDER_CREATED"),
                "REJECTED", terminalTimer(meters, "REJECTED"),
                "SYSTEM_FAILED", terminalTimer(meters, "SYSTEM_FAILED"));
    }

    @Transactional
    public void process(OrderCommand command) {
        long beforeLockQuery = System.nanoTime();
        LockedRequest request = lockMatchingRequest(command);
        if (request == null) {
            throw new OrderCommand.Invalid("Order command does not match a stored purchase request");
        }
        if ("ACCEPTED".equals(request.status())) {
            PurchaseResult result = orders.attemptOrder(
                    request.id(), request.requestUid(), request.campaignId(), request.userId());
            recordTerminalAfterCommit(request.acceptedAgeMicros(), beforeLockQuery, result.status());
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recover(ConsumerRecord<?, ?> record, Exception failure) {
        OrderCommand command = readRecoverableCommand(record);
        long beforeLockQuery = System.nanoTime();
        LockedRequest request = command == null ? null : lockMatchingRequest(command);
        if (request != null) {
            if (!"ACCEPTED".equals(request.status())) {
                return;
            }
            if (commands.markSystemFailed(request.id()) != 1) {
                throw new IllegalStateException("Could not record failure for " + request.requestUid());
            }
        }
        commands.insertDeadLetter(deadLetter(record, failure, request));
        if (request != null) {
            recordTerminalAfterCommit(request.acceptedAgeMicros(), beforeLockQuery, "SYSTEM_FAILED");
        }
    }

    private OrderCommand readRecoverableCommand(ConsumerRecord<?, ?> record) {
        try {
            return OrderCommand.read(json, record);
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

    private void recordTerminalAfterCommit(long acceptedAgeMicros, long beforeLockQuery, String status) {
        Timer timer = terminalLatency.get(status);
        if (timer == null) {
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                // MySQL evaluates NOW at statement start. Approximate through the app-observed commit
                // with monotonic elapsed time from just before that statement, including its outbound trip.
                long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - beforeLockQuery);
                timer.record(Math.max(0, acceptedAgeMicros + elapsedMicros), TimeUnit.MICROSECONDS);
            }
        });
    }

    private static Timer terminalTimer(MeterRegistry meters, String status) {
        return Timer.builder("flashorder.requests.terminal.latency")
                .description("Estimated time from durable request acceptance through the terminal commit")
                .tag("status", status)
                .publishPercentileHistogram()
                .maximumExpectedValue(Duration.ofHours(1))
                .register(meters);
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
