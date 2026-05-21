# Event-Driven Microservices System

A production-style event-driven microservices project built using:

- Java 17
- Spring Boot 3
- RabbitMQ
- Apache Kafka
- Spring Data JPA
- Transactional Outbox Pattern
- Docker & Docker Compose

---

# Architecture Overview

The system contains two microservices:

| Service | Responsibility |
|---|---|
| Order Service | Creates orders and publishes events |
| Notification Service | Consumes events and sends notifications |

---

# Features

## Order Service

- REST API to create orders
- Saves orders in database
- Implements Transactional Outbox Pattern
- Publishes events to RabbitMQ
- Sends audit logs to Kafka
- Uses durable RabbitMQ queues
- Uses RabbitMQ Publisher Confirms
- Uses Kafka `acks=all`

---

## Notification Service

- Consumes RabbitMQ events
- Sends notifications
- Implements idempotency
- Uses manual RabbitMQ ACKs
- Uses Dead Letter Queue (DLQ)
- Sends audit logs to Kafka
- Uses Kafka `acks=all`

---

## Bonus Features

- Kafka Audit Consumer
- Manual Kafka offset acknowledgment
- Unit Testing with Mockito
- Integration Testing with `@SpringBootTest`

---

# System Flow

```text
Client
   |
POST /orders
   |
Order Service
   |
+--------------------------+
| Database Transaction     |
|--------------------------|
| Save Order               |
| Save Outbox Event        |
+--------------------------+
   |
Outbox Publisher
   |
RabbitMQ Queue
   |
Notification Service
   |
+--------------------------+
| Idempotency Check        |
| Manual ACK               |
| DLQ on Failure           |
+--------------------------+
   |
Kafka Audit Logs
```

---

# Project Structure

```text
event-driven-system/
│
├── pom.xml
├── docker-compose.yml
├── README.md
│
├── order-service/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
│
└── notification-service/
    ├── Dockerfile
    ├── pom.xml
    └── src/
```

---

# Technologies Used

| Technology | Purpose |
|---|---|
| Spring Boot | Microservices Framework |
| RabbitMQ | Event Messaging |
| Apache Kafka | Audit Logging |
| Spring Data JPA | Database Access |
| MySQL Database | Local Development DB |
| Docker | Containerization |
| Docker Compose | Infrastructure Orchestration |
| JUnit 5 | Testing |
| Mockito | Unit Testing |

---

# Transactional Outbox Pattern

The Order Service uses the Transactional Outbox Pattern to guarantee reliable event publishing.

## Why?

Without the outbox pattern:

```text
1. Save order to DB
2. Publish RabbitMQ event
```

If step 2 fails, the database and messaging system become inconsistent.

---

## Solution

Inside ONE database transaction:

```text
1. Save Order
2. Save Outbox Event
```

A scheduler later publishes pending events safely.

---

# RabbitMQ Reliability

The system implements:

## Durable Queue

```java
QueueBuilder.durable("order.queue")
```

Ensures queue survives RabbitMQ restart.

---

## Durable Messages

Messages are persisted to disk.

---

## Publisher Confirms

RabbitMQ confirms that messages are received successfully.

```yaml
spring:
  rabbitmq:
    publisher-confirm-type: correlated
```

---

# Kafka Reliability

Kafka producer configuration:

```yaml
spring:
  kafka:
    producer:
      acks: all
```

This guarantees Kafka waits for all replicas before confirming success.

---

# Idempotency

The Notification Service prevents duplicate processing.

Each event contains:

```text
id
```

Processed event IDs are stored in DB.

If the same message arrives again:

```text
Duplicate message ignored
```

---

# Dead Letter Queue (DLQ)

Failed messages are routed automatically to:

```text
order.dlq
```

This prevents infinite retry loops.

---

# Manual ACKs

RabbitMQ messages are acknowledged ONLY after successful processing.

```java
channel.basicAck(tag, false);
```

If processing fails:

```java
channel.basicReject(tag, false);
```

The message goes to the DLQ.

---

# Prerequisites

Install:

- Docker
- Docker Compose

---

# Running the Project

## 1. Clone Repository

```bash
git clone https://github.com/MahmoudRezk1/E-Commerce-Microservices
```

```bash
cd E-Commerce-Microservices
```

---

## 2. Run Everything with Docker Compose

```bash
docker-compose up --build
```

This starts:

- RabbitMQ
- Kafka
- Zookeeper
- Order Service
- Notification Service

---

# Services

| Service | URL |
|---|---|
| Order Service | http://localhost:8081 |
| Notification Service | http://localhost:8082 |
| RabbitMQ Management | http://localhost:15672 |

---

# RabbitMQ Credentials

```text
Username: guest
Password: guest
```

---

# Creating an Order

## Request

```bash
curl --location 'http://localhost:8081/api/v1/orders' \
--header 'Content-Type: application/json' \
--data '{
  "customerId": "VIP-CLIENT-105",
  "totalAmount": 299.99
}'
```

---

## Example Response

```json
{
    "id": 6,
    "customerId": "VIP-CLIENT-105",
    "totalAmount": 299.99,
    "status": "CREATED"
}
```

---

# Expected Console Logs

## Order Service

```text
RabbitMQ ACK received for outbox event ID: 6
Kafka Audit log dispatched for Order: 6
```

---

## Notification Service

```text
Notification sent for order ID: 6
```

---

## Kafka Audit Consumer

```text
Received Log Record -> Key: 6, Value: Notification successfully sent for order: 6, Partition: 0
Received Log Record -> Key: 6, Value: Order created for ID: 6, Partition: 0
```

---

# Testing

---

# Unit Tests

Unit tests use Mockito to isolate business logic.

## Run Unit Tests

```bash
mvn test
```

---

# Integration Tests

Integration tests use:

```java
@SpringBootTest
```

To test the REST API and Spring context.

---

## Example Integration Test

```java
mockMvc.perform(
    post("/api/v1/orders")
)
.andExpect(status().created());
```

---

# Testing Scenarios

---

## 1. Happy Path

### Steps

1. Create order
2. Verify:
    - Order saved
    - Outbox event saved
    - RabbitMQ event published
    - Notification printed
    - Kafka audit log created

---

## 2. Duplicate Message Test

### Goal

Verify idempotency.

### Steps

1. Publish same message twice
2. Verify second message ignored

Expected log:

```text
Duplicate processing detected! Order message ID <Message ID> already executed.
```

---

## 3. DLQ Test

### Goal

Verify failed messages move to DLQ.

### Steps

in case of any issue or exception in messages consuming:

```java
catch (Exception e) {
            log.error("Critical failure during message processing execution. Routing to DLQ.", e);
            try {
                // Reject message explicitly, requeue = false routes it cleanly to the configured DLQ
                channel.basicReject(tag, false);
            } catch (Exception rejectException) {
                log.error("Fatal broker connection error while issuing basicReject.", rejectException);
            }
        }
```

### Verify

Message appears in:

```text
order.dlq
```

---

## 4. RabbitMQ Durability Test

### Steps

1. Stop RabbitMQ container
2. Start RabbitMQ again
3. Verify queue/messages still exist

---

## 5. Kafka Reliability Test

### Verify

Kafka audit messages continue appearing even after broker restart.

---

# Dockerfiles

Each service contains its own Dockerfile.

Example:

```dockerfile
FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /app

COPY . .

RUN mvn clean package -pl notification-service -am -DskipTests

FROM eclipse-temurin:17-jdk

WORKDIR /app

COPY --from=build /app/notification-service/target/*.jar notification.jar

EXPOSE 8082

ENTRYPOINT ["java", "-jar", "notification.jar"]
```

---

# Docker Compose Services

The project includes:

| Container | Purpose |
|---|---|
| rabbitmq | Messaging |
| kafka | Audit logs |
| zookeeper | Kafka dependency |
| order-service | Producer service |
| notification-service | Consumer service |

---

# Important Design Decisions

---

## Why RabbitMQ?

RabbitMQ is ideal for:

- Reliable queues
- Consumer acknowledgments
- DLQ support
- Work distribution

---

## Why Kafka?

Kafka is ideal for:

- Audit logs
- Event streaming
- High throughput
- Replay capability

---

## Why Outbox Pattern?

Guarantees consistency between:

- Database state
- Published events

---

# Useful Commands

---

## Build Project

```bash
mvn clean package
```

---

## Run Unit Tests

```bash
mvn test
```

---

## Run Specific Module

### Order Service

```bash
cd order-service
mvn spring-boot:run
```

---

### Notification Service

```bash
cd notification-service
mvn spring-boot:run
```

---

## Stop Containers

```bash
docker-compose down
```

---

# API Reference

---

## Create Order

### Endpoint

```http
POST /api/v1/orders
```

---

## Request Body

```json
{
  "customerId": "VIP-CLIENT-105",
  "totalAmount": 299.99
}
```

---

## Response

```json
{
    "id": 6,
    "customerId": "VIP-CLIENT-105",
    "totalAmount": 299.99,
    "status": "CREATED"
}
```

---

# Author

Senior Java: Mahmoud Rezk Barakat
