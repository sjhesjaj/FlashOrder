package com.flashorder.request;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration(proxyBeanMethods = false)
@Profile("consumer")
@ConditionalOnProperty(name = "flashorder.intake-mode", havingValue = "async")
public class OrderCommandConsumerConfiguration {

    @Bean
    CommonErrorHandler orderCommandErrorHandler(OrderCommandProcessingService processing) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(100);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(1_000);
        DefaultErrorHandler handler = new DefaultErrorHandler(processing::recover, backOff);
        handler.addNotRetryableExceptions(OrderCommand.Invalid.class);
        return handler;
    }
}
