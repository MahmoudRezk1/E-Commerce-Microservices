package com.saqaya.notificationservice.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaAuditConsumer implements AcknowledgingMessageListener<String, String> {

    /**
     * Invoked with data from kafka.
     *
     * @param data           the data to be processed.
     * @param acknowledgment the acknowledgment.
     */
    @Override
    @KafkaListener(topics = "audit-logs", groupId = "audit-consumer-group", containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
        try {
            log.info("Received Log Record -> Key: {}, Value: {}, Partition: {}", data.key(), data.value(), data.partition());

            // Manual commit offset confirmation
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Failed handling partition frame during audit verification logging context.", e);
        }
    }
}
