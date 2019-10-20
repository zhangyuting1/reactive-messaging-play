package com.demo.reactivemessagingplay.producer;

import lombok.extern.slf4j.Slf4j;
import com.demo.reactivemessagingplay.common.domain.EventType;
import com.demo.reactivemessagingplay.common.domain.Product;
import com.demo.reactivemessagingplay.common.domain.ProductEvent;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.reactive.FluxSender;
import org.springframework.cloud.stream.reactive.StreamEmitter;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.UUID;

@SpringBootApplication
@EnableBinding(Source.class)
@Slf4j
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @StreamEmitter
    @Output(Source.OUTPUT)
    public void emmit(FluxSender fluxSender) {
        fluxSender.send(Flux.interval(Duration.ofSeconds(1)).map(id -> {

            final Product product = new Product();
            product.setId(id);
            product.setName(RandomStringUtils.randomAlphanumeric(5, 10));

            final ProductEvent event = new ProductEvent();
            event.setId(UUID.randomUUID().toString());
            event.setProduct(product);
            if (id % 3 == 0) {
                event.setEventType(EventType.DELETE);
            } else if (id % 2 == 0) {
                event.setEventType(EventType.UPDATE);
            } else {
                event.setEventType(EventType.CREATE);
            }

            log.info("producing product event: {}", event);

            return event;
        }));
    }

}
