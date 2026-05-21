package com.saqaya.orderservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.saqaya.orderservice.entity.Order;
import com.saqaya.orderservice.entity.OutboxEvent;
import com.saqaya.orderservice.enums.Status;
import com.saqaya.orderservice.repository.OrderRepository;
import com.saqaya.orderservice.repository.OutboxRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public Order createOrder(Order order){
        order.setStatus(Status.CREATED);
        Order savedOrder = orderRepository.save(order);

        try{
            String payload = objectMapper.writeValueAsString(savedOrder);
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType("ORDER")
                    .aggregateId(savedOrder.getId().toString())
                    .eventType("OrderCreatedEvent")
                    .payload(payload)
                    .status(Status.PENDING)
                    .createdAt(LocalDateTime.now())
                    .build();
            outboxRepository.save(outboxEvent);
        }catch (Exception e){
            throw new RuntimeException("Failed to serialize outbox payload", e);
        }

        return savedOrder;
    }
}
