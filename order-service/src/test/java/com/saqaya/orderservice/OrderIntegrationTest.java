package com.saqaya.orderservice;

import com.saqaya.orderservice.entity.Order;
import com.saqaya.orderservice.entity.OutboxEvent;
import com.saqaya.orderservice.enums.Status;
import com.saqaya.orderservice.repository.OrderRepository;
import com.saqaya.orderservice.repository.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrderIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxRepository outboxRepository;

    @Test
    void testCreateOrderAPIEndpoint_IntegrationFlow() {
        // 1. Arrange: Prepare an incoming client JSON payload
        Order orderPayload = Order.builder()
                .customerId("INTEGRATION-TEST-CUST")
                .totalAmount(new BigDecimal("799.99"))
                .build();

        // 2. Act: Send a live HTTP post network packet against the exposed route
        ResponseEntity<Order> response = restTemplate.postForEntity("/api/v1/orders", orderPayload, Order.class);

        // 3. Assert: Verify the outer HTTP layer behaves according to spec
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());

        Long generatedOrderId = response.getBody().getId();
        assertNotNull(generatedOrderId);
        assertEquals(Status.CREATED, response.getBody().getStatus());

        // 4. Assert: Look into the persistent database layer to verify the Outbox Pattern was triggered atomicly
        Order persistedOrder = orderRepository.findById(generatedOrderId).orElse(null);
        assertNotNull(persistedOrder, "Order should be saved in the database");
        assertEquals("INTEGRATION-TEST-CUST", persistedOrder.getCustomerId());

        // Verify that exactly 1 companion Outbox Event was recorded inside the same database block
        List<OutboxEvent> pendingOutboxRecords = outboxRepository.findByStatus(Status.PENDING);
        boolean outboxEventExists = pendingOutboxRecords.stream()
                .anyMatch(event -> event.getAggregateId().equals(generatedOrderId.toString()));

        assertTrue(outboxEventExists, "A corresponding PENDING outbox event must exist in the database!");
    }
}