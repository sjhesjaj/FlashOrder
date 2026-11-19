package com.flashorder.request;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
@Profile("consumer")
@ConditionalOnProperty(name = "flashorder.intake-mode", havingValue = "async")
public class OrderCommandListener {

    private final JsonMapper json;
    private final OrderCommandProcessingService processing;

    public OrderCommandListener(JsonMapper json, OrderCommandProcessingService processing) {
        this.json = json;
        this.processing = processing;
    }

    @KafkaListener(id = "flashorder-order-commands", idIsGroup = false, topics = "flashorder.order-commands")
    public void onCommand(ConsumerRecord<String, String> record) {
        processing.process(OrderCommand.read(json, record));
    }
}
