package com.flashorder.request;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public record OrderCommand(String eventId, long requestId, String requestUid, long campaignId) {

    static OrderCommand read(JsonMapper json, ConsumerRecord<?, ?> record) {
        if (!(record.value() instanceof String payload)) {
            throw new Invalid("Order command has no text payload");
        }
        JsonNode node;
        try {
            node = json.readTree(payload);
        } catch (JacksonException exception) {
            throw new Invalid("Order command contains invalid JSON", exception);
        }
        if (node == null || !node.isObject()) {
            throw new Invalid("Order command must be a JSON object");
        }
        long campaignId = readPositiveId(node, "campaignId");
        if (!Long.toString(campaignId).equals(record.key())) {
            throw new Invalid("Order command key does not match the campaign");
        }
        return new OrderCommand(readText(node, "eventId"), readPositiveId(node, "requestId"),
                readText(node, "requestUid"), campaignId);
    }

    private static long readPositiveId(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() <= 0) {
            throw new Invalid(field + " must be a positive integer");
        }
        return value.longValue();
    }

    private static String readText(JsonNode node, String field) {
        String value = node.path(field).stringValue("");
        if (value.isBlank()) {
            throw new Invalid(field + " is required");
        }
        return value;
    }

    static final class Invalid extends RuntimeException {
        Invalid(String message) {
            super(message);
        }

        Invalid(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
