package com.demo.reactivemessagingplay.notifier;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.demo.reactivemessagingplay.common.domain.Constants;
import com.demo.reactivemessagingplay.common.domain.Product;
import com.demo.reactivemessagingplay.common.domain.ProductEvent;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.ReplayProcessor;

import javax.annotation.PostConstruct;
import java.util.Arrays;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@SpringBootApplication
public class NotifierApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotifierApplication.class, args);
    }

    @Bean
    public ReactiveRedisMessageListenerContainer container(ReactiveRedisConnectionFactory factory) {
        return new ReactiveRedisMessageListenerContainer(factory);
    }

    @Bean
    public RouterFunction<ServerResponse> routers(NotifyService notifyService) {
        return route(GET("/events"),
                     serverRequest -> ServerResponse.ok()
                                                    .body(BodyInserters.fromServerSentEvents(notifyService.notifyEvents())));
    }

    @Service
    @Slf4j
    @RequiredArgsConstructor
    public static class NotifyService {

        private final ReactiveRedisMessageListenerContainer container;

        private final ReplayProcessor<ProductEvent> processor = ReplayProcessor.create();

        @PostConstruct
        public void init() {

            final FluxSink<ProductEvent> sink = processor.sink();

            container.receive(Arrays.asList(ChannelTopic.of(Constants.PRODUCT_EVENT_TOPIC)),
                              RedisSerializationContext.SerializationPair.fromSerializer(new StringRedisSerializer()),
                              RedisSerializationContext.SerializationPair.fromSerializer(new Jackson2JsonRedisSerializer<>(ProductEvent.class)))
                     .map(ReactiveSubscription.Message::getMessage)
                     .doOnNext(event -> log.info("publishing event: {}", event))
                     .doOnNext(event -> sink.next(event))
                     .subscribe(t -> {}, e -> {});

        }

        public Flux<ServerSentEvent<Product>> notifyEvents() {

            return processor.map(event -> ServerSentEvent.<Product>builder()
                    .id(event.getId())
                    .data(event.getProduct())
                    .event(event.getEventType().name())
                    .build());

        }

    }

}
