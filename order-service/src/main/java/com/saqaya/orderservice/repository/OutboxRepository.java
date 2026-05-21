package com.saqaya.orderservice.repository;

import com.saqaya.orderservice.entity.OutboxEvent;
import com.saqaya.orderservice.enums.Status;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEvent,Long> {
    List<OutboxEvent> findByStatus(Status status);
}
