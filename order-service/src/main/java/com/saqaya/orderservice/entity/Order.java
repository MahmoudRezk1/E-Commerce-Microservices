package com.saqaya.orderservice.entity;

import com.saqaya.orderservice.enums.Status;
import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;

@Entity
@Table(name = "orders")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String customerId;
    private BigDecimal totalAmount;
    @Enumerated(EnumType.STRING)
    private Status status;
}
