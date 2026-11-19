package com.flashorder.common.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration(proxyBeanMethods = false)
@Profile("scheduler | consumer")
@ConditionalOnProperty(name = "flashorder.intake-mode", havingValue = "async")
public class OrderTopics {
    @Bean
    KafkaAdmin.NewTopics flashOrderTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("flashorder.order-commands").partitions(6).replicas(1).build(),
                TopicBuilder.name("flashorder.order-events").partitions(6).replicas(1).build(),
                TopicBuilder.name("flashorder.order-commands.DLT").partitions(6).replicas(1).build());
    }
}
