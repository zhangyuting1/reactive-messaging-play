package com.demo.reactivemessagingplay.common.domain;

import lombok.Data;

@Data
public class ProductEvent {

    private String id;

    private EventType eventType;

    private Product product;

}
