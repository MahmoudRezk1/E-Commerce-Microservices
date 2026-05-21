package com.saqaya.orderservice.publisher;

import com.saqaya.orderservice.config.RabbitConfig;
import com.saqaya.orderservice.entity.OutboxEvent;
import com.saqaya.orderservice.enums.Status;
import com.saqaya.orderservice.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
@EnableScheduling
public class OutboxPublisher {
    private final OutboxRepository outboxRepository;
    private final RabbitTemplate rabbitTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 5000)
    public void processOutbox() {
        List<OutboxEvent> pendingEvents = outboxRepository.findByStatus(Status.PENDING);
        for (OutboxEvent event : pendingEvents) {
            try {
                CorrelationData correlationData = new CorrelationData(event.getId().toString());
                rabbitTemplate.convertAndSend(
                        RabbitConfig.ORDER_EXCHANGE,
                        RabbitConfig.ORDER_ROUTING_KEY,
                        event.getPayload(),
                        correlationData
                );

                correlationData.getFuture().whenComplete((confirm, throwable) -> {
                    if (throwable == null && confirm != null && confirm.isAck()) {
                        event.setStatus(Status.PROCESSED);
                        outboxRepository.save(event);
                        log.info("RabbitMQ ACK received for outbox event ID: {}", event.getId());
                    } else {
                        event.setStatus(Status.FAILED);
                        outboxRepository.save(event);
                        log.error("RabbitMQ NACK or exception received for outbox event ID: {}", event.getId());
                    }
                });

                kafkaTemplate
                        .send(
                                "audit-logs", event.getAggregateId(),
                                "Order created for ID: " + event.getAggregateId())
                        .get();
                log.info("Kafka Audit log dispatched for Order: {}", event.getAggregateId());

            } catch (Exception e) {
                log.error("Failed to relay outbox event ID: {}", event.getId(), e);
                event.setStatus(Status.FAILED);
                outboxRepository.save(event);
            }
        }
    }
}
