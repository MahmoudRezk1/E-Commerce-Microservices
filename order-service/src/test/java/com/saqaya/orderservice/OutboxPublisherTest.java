package com.saqaya.orderservice;

import com.saqaya.orderservice.config.RabbitConfig;
import com.saqaya.orderservice.entity.OutboxEvent;
import com.saqaya.orderservice.enums.Status;
import com.saqaya.orderservice.publisher.OutboxPublisher;
import com.saqaya.orderservice.repository.OutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class OutboxPublisherTest {
    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private RabbitTemplate rabbitTemplate;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @InjectMocks
    private OutboxPublisher outboxPublisher;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    @DisplayName("Unit Test: Outbox poller should fetch PENDING events, attach correlation IDs, and publish to RabbitMQ and Kafka")
    void testProcessOutbox_SuccessFlow() {
        // 1. Arrange: Create a mock database outbox event record
        OutboxEvent pendingEvent = OutboxEvent.builder()
                .id(101L)
                .aggregateType("ORDER")
                .aggregateId("42")
                .eventType("OrderCreatedEvent")
                .payload("{\"id\":42,\"customerId\":\"VIP-USER\"}")
                .status(Status.PENDING)
                .createdAt(LocalDateTime.now())
                .build();

        List<OutboxEvent> mockPendingList = Collections.singletonList(pendingEvent);
        when(outboxRepository.findByStatus(Status.PENDING)).thenReturn(mockPendingList);

        CompletableFuture<SendResult<String, String>> mockKafkaFuture =
                CompletableFuture.completedFuture(mock(SendResult.class));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(mockKafkaFuture);

        outboxPublisher.processOutbox();

        ArgumentCaptor<CorrelationData> correlationCaptor = ArgumentCaptor.forClass(CorrelationData.class);

        verify(rabbitTemplate, times(1)).convertAndSend(
                eq(RabbitConfig.ORDER_EXCHANGE),
                eq(RabbitConfig.ORDER_ROUTING_KEY),
                eq(pendingEvent.getPayload()),
                correlationCaptor.capture()
        );

        CorrelationData capturedCorrelation = correlationCaptor.getValue();
        assertEquals("101", capturedCorrelation.getId());

        verify(kafkaTemplate, times(1)).send(
                eq("audit-logs"),
                eq("42"),
                eq("Order created for ID: 42")
        );
    }
}
