package com.saqaya.notificationservice.consumer;

import com.saqaya.notificationservice.entity.ProcessedMessage;
import com.saqaya.notificationservice.repository.ProcessedMessageRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderNotificationConsumer {
    private final ProcessedMessageRepository processedMessageRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @RabbitListener(queues = {"order.queue"})
    public void consumeOrderEvent(
            String payload,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long tag
    ) {
        try {
            JsonNode jsonNode = objectMapper.readTree(payload);
            String orderId = jsonNode.get("id").asText();

            // Idempotency check
            if (processedMessageRepository.existsById(orderId)) {
                log.warn("Duplicate processing detected! Order message ID {} already executed.", orderId);
                channel.basicAck(tag, false); // ACK the message to clear it from the queue
                return;
            }

            // Execute core processing logic
            log.info("Notification sent for order ID: {}", orderId);

            // Record message identity within database context before ACK boundary
            processedMessageRepository.save(new ProcessedMessage(orderId, LocalDateTime.now()));

            // Relay Audit update forward to Kafka topic
            kafkaTemplate
                    .send(
                            "audit-logs",
                            orderId,
                            "Notification successfully sent for order: " + orderId)
                    .get();

            // Explicitly ACK back up stream
            channel.basicAck(tag, false);

        } catch (Exception e) {
            log.error("Critical failure during message processing execution. Routing to DLQ.", e);
            try {
                // Reject message explicitly, requeue = false routes it cleanly to the configured DLQ
                channel.basicReject(tag, false);
            } catch (Exception rejectException) {
                log.error("Fatal broker connection error while issuing basicReject.", rejectException);
            }
        }
    }
}
