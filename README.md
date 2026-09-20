# 🚀 Spring Boot & Apache Kafka Real-World Event-Driven Microservices

A real-world implementation of a scalable, fault-tolerant **Event-Driven Architecture (EDA)** built with **Java**, **Spring Boot**, and **Apache Kafka**. This project handles real-time data ingestion from a live streaming source, processes records asynchronously through a distributed event broker, and persists them into a persistent MySQL data store.

---

## 🏗️ Architecture Overview

The system architecture consists of two decoupled, independent microservices communicating asynchronously via an Apache Kafka broker network:

### 1. `kafka-producer-wikimedia` (Microservice 1)

* Establishes a reactive streaming connection to the live public **Wikimedia Recent Changes API** using an open HTTP EventSource network connection.
* Captures high-volume event streams asynchronously and publishes payloads line-by-line onto a dedicated Kafka cluster topic.

### 2. `kafka-consumer-database` (Microservice 2)

* Subscribes to the Kafka event channel topics within a managed consumer partition group sharing a unified group ID.
* Constantly polls incoming system payloads, processing text/JSON streams gracefully without losing track of message offsets.
* Maps processed items into relational JPA structural entities using custom configurations and persists records to a **MySQL** target database.

---

## 🛠️ Core Engineering Features

* **Decoupled System Scaling:** Decentralized architecture enabling producers and consumers to fail or scale independently without direct interaction.
* **Event Ingestion Pipelines:** Asynchronous text/JSON data processing streams directly handling complex user modifications live from a public source.
* **Spring Boot Auto-Configuration:** Leverages advanced Spring Kafka framework dependencies to eliminate extensive boilerplate messaging infrastructure setup code.
* **Enterprise Persistence Isolation:** Custom integration utilizing Spring Data JPA (with underlying Hibernate mapping dialects) to seamlessly structure and batch-insert raw data columns dynamically.

---

## 💻 Tech Stack & Dependencies

* **Language/Runtime:** Java (Standard JVM configuration)
* **Framework:** Spring Boot, Spring for Apache Kafka, Spring Data JPA
* **Message Broker:** Apache Kafka Core (Distributed Cluster Ecosystem, Broker Servers)
* **Database System:** MySQL Database System
* **Networking Tools:** OkHTTP EventSource Client Utilities
* **Utilities:** Project Lombok, Jackson JSON Core Tools

---

## 🐳 Automated Local Infrastructure (Docker Compose)

To simplify local development and avoid manual installations, a `docker-compose.yml` script is provided to automatically spin up a multi-container environment consisting of **Apache Kafka (KRaft mode)** and **MySQL 8**.

### 1. Environment Configuration Script

Create a `docker-compose.yml` file in the root directory of your project:

```yaml
version: '3.8'

services:
  mysql:
    image: mysql:8.0
    container_name: wikimedia-mysql
    ports:
      - "3306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: rootpassword
      MYSQL_DATABASE: wikimedia
    volumes:
      - mysql_data:/var/lib/mysql
    networks:
      - eda-network

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: wikimedia-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
    networks:
      - eda-network

volumes:
  mysql_data:

networks:
  eda-network:
    driver: bridge
```

---

## ⚙️ Configuration Setup

### Microservice 1: `kafka-producer-wikimedia`

Configure your `application.properties` file in `src/main/resources`:

```properties
spring.kafka.producer.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
```

### Microservice 2: `kafka-consumer-database`

Configure your `application.properties` file in `src/main/resources`:

```properties
# Kafka Consumer Configuration
spring.kafka.consumer.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=my-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# Database Configuration
spring.datasource.url=jdbc:mysql://localhost:3306/wikimedia?useSSL=false&allowPublicKeyRetrieval=true
spring.datasource.username=root
spring.datasource.password=rootpassword

# JPA/Hibernate Properties
spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.MySQL8Dialect
spring.jpa.hibernate.ddl-auto=update
spring.jpa.properties.hibernate.show_sql=true
spring.jpa.properties.hibernate.format_sql=true
```

---

## 🚦 Getting Started & Execution Flow

### 1. Boot up the Docker Containers

Open your terminal at the root directory of the project and launch the Kafka broker and MySQL server instances:

```bash
docker compose up -d
```

### 2. Build the Multi-Module Project

Compile the parent project along with its child microservice components via Maven:

```bash
mvn clean install
```

### 3. Run the Applications

Launch both microservices concurrently to activate event-driven messaging:

* **Run Consumer Database Microservice:**
  ```bash
  java -jar kafka-consumer-database/target/kafka-consumer-database-0.0.1-SNAPSHOT.jar
  ```

* **Run Producer Wikimedia Microservice:**
  ```bash
  java -jar kafka-producer-wikimedia/target/kafka-producer-wikimedia-0.0.1-SNAPSHOT.jar
  ```

Once active, the producer pipelines stream data from Wikimedia directly into Kafka. Simultaneously, the consumer polls the cluster, deserializes the properties, and writes records into the target database.
