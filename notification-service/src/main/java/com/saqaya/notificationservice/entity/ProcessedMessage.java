package com.saqaya.notificationservice.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "processed_messages")
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProcessedMessage {
    @Id
    private String messageId; // Maps directly to Order ID inside the payload context
    private LocalDateTime processedAt;
}
