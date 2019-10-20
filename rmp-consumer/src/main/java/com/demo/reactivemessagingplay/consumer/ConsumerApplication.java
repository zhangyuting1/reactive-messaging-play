package com.demo.reactivemessagingplay.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.demo.reactivemessagingplay.common.domain.Constants;
import com.demo.reactivemessagingplay.common.domain.ProductEvent;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@SpringBootApplication
@EnableBinding(Sink.class)
public class ConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }

    @Bean
    public ReactiveRedisOperations<String, ProductEvent> productEventReactiveRedisOperations(ReactiveRedisConnectionFactory factory) {
        final Jackson2JsonRedisSerializer<ProductEvent> serializer = new Jackson2JsonRedisSerializer<>(ProductEvent.class);

        RedisSerializationContext.RedisSerializationContextBuilder<String, ProductEvent> builder =
                RedisSerializationContext.newSerializationContext(new StringRedisSerializer());
        final RedisSerializationContext<String, ProductEvent> context = builder.value(serializer).build();

        return new ReactiveRedisTemplate<>(factory, context);
    }

    @Service
    @Slf4j
    @RequiredArgsConstructor
    public static class ConsumerService {

        private final ReactiveRedisOperations<String, ProductEvent> reactiveRedisOperations;

        @StreamListener
        public void consume(@Input(Sink.INPUT) Flux<ProductEvent> events) {
            events.doOnNext(event -> log.info("consuming product event: {}", event))
                  .flatMap(event -> reactiveRedisOperations.convertAndSend(Constants.PRODUCT_EVENT_TOPIC, event))
                  .subscribe(t -> {}, e -> {});
        }
    }

}