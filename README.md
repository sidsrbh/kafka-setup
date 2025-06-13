# Go Kafka Example with Producer, Consumers, Admin Client, and DLQ

This project demonstrates how to interact with Apache Kafka using Go, featuring a producer, multiple consumer implementations (including one with robust error handling and a Dead Letter Queue), and an admin client for topic management. The entire setup runs locally using Docker Compose.

---

## Table of Contents

- [Go Kafka Example with Producer, Consumers, Admin Client, and DLQ](#go-kafka-example-with-producer-consumers-admin-client-and-dlq)
  - [Table of Contents](#table-of-contents)
  - [Project Overview](#project-overview)
  - [Features](#features)
  - [Prerequisites](#prerequisites)
  - [Getting Started](#getting-started)
    - [1. Start Kafka and Zookeeper](#1-start-kafka-and-zookeeper)
    - [2. Create Kafka Topics](#2-create-kafka-topics)
    - [3. Run the Producer](#3-run-the-producer)
    - [4. Run the Consumers](#4-run-the-consumers)
      - [Basic Consumer (`consumer.go`)](#basic-consumer-consumergo)
      - [Manual Offset Consumer (`consumer.go - Manual Offset Management`)](#manual-offset-consumer-consumergo---manual-offset-management)
      - [DLQ Consumer (`consumer.go - Robust Error Handling with DLQ`)](#dlq-consumer-consumergo---robust-error-handling-with-dlq)
      - [Consumer Reading Headers (`consumer.go - Reading Message Headers`)](#consumer-reading-headers-consumergo---reading-message-headers)
  - [Code Explanation](#code-explanation)
    - [`docker-compose.yml`](#docker-composeyml)
    - [`admin_client.go`](#admin_clientgo)
    - [`producer.go`](#producergo)
    - [`consumer.go` (Basic)](#consumergo-basic)
    - [`consumer.go` (Manual Offset Management)](#consumergo-manual-offset-management)
    - [`consumer.go` (Robust Error Handling with DLQ)](#consumergo-robust-error-handling-with-dlq)
    - [`consumer.go` (Reading Message Headers)](#consumergo-reading-message-headers)
  - [Cleanup](#cleanup)
  - [Troubleshooting](#troubleshooting)
  - [Contributing](#contributing)
  - [License](#license)

---

## Project Overview

This project provides a comprehensive set of examples for building Kafka applications in Go. It includes:

1.  **Kafka Infrastructure**: A `docker-compose.yml` to quickly spin up a Kafka broker and Zookeeper.
2.  **Admin Client**: A Go program to programmatically create Kafka topics.
3.  **Producer**: A Go program to send messages to a Kafka topic, including examples of adding message headers.
4.  **Consumers**:
    * **Basic Consumer**: A simple consumer that fetches and logs messages with automatic offset commits.
    * **Manual Offset Consumer**: Demonstrates how to manually commit offsets after successful message processing, useful for ensuring "at-least-once" delivery semantics and handling failures.
    * **DLQ Consumer**: Implements a Dead Letter Queue (DLQ) pattern for handling messages that fail processing after multiple retries.
    * **Consumer with Headers**: Shows how to read custom headers from Kafka messages.

---

## Features

* **Dockerized Kafka**: Easy setup of a local Kafka environment.
* **Topic Creation**: Programmatic topic creation using `kafka-go`'s admin client.
* **Message Production**: Sending messages to Kafka with custom keys, values, and headers.
* **Flexible Consumption**: Multiple consumer examples showcasing:
    * Automatic vs. Manual Offset Commits.
    * Graceful shutdown using `context` and OS signals.
    * Simulated processing failures and retry logic.
    * Dead Letter Queue (DLQ) implementation for problematic messages.
    * Reading and interpreting message headers.
* **Structured Go Code**: Clear and commented Go code demonstrating best practices for Kafka integration.

---

## Prerequisites

Before you begin, ensure you have the following installed:

* **Go**: Version 1.16 or higher.
* **Docker** and **Docker Compose**: For running Kafka and Zookeeper locally.

---

## Getting Started

Follow these steps to set up and run the Kafka examples.

### 1. Start Kafka and Zookeeper

Navigate to the root directory of the project where `docker-compose.yml` is located and start the Kafka services:

```bash
docker-compose up -d
```

This will start a Zookeeper instance on `localhost:2181` and a Kafka broker on `localhost:9092`.

### 2. Create Kafka Topics

The `admin_client.go` program will create the necessary topics.

First, ensure you have the `segmentio/kafka-go` library installed:

```bash
go get [github.com/segmentio/kafka-go](https://github.com/segmentio/kafka-go)
```

Then, run the admin client to create the `Ramayana` and `Ramayana-dlq` topics:

```bash
go run admin_client.go
```

You should see output similar to:

```
Creating topic 'Ramayana-dlq' with 3 partitions and 1 replicas...
Topic 'Ramayana-dlq' created successfully!
Creating topic 'Ramayana' with 3 partitions and 1 replicas...
Topic 'Ramayana' created successfully!
```

### 3. Run the Producer

The `producer.go` program will send messages to the `Ramayana` topic.

```bash
go run producer.go
```

You will see messages being produced to the console, including their keys, values, and headers:

```
Kafka Producer started. Sending messages with headers...
Produced message | Key: Key-0 | Value: Hello Kafka from Go! Message 0 at 2025-06-13T16:07:39+05:30 | Headers: [{X-Trace-ID trace-1718285259-0} {X-Source-Service go-producer-app} {X-Timestamp 1718285259123}]
...
```

Let this producer run in a separate terminal.

### 4. Run the Consumers

You can run different consumer implementations in separate terminal windows to observe their behavior.

#### Basic Consumer (`consumer.go`)

This consumer uses the default automatic offset commitment.

```bash
# Rename the first consumer.go to avoid conflicts
mv consumer.go consumer_basic.go
go run consumer_basic.go
```

You should see messages being consumed:

```
Kafka Consumer for topic 'Ramayana', group 'Kids-of-India' started. Reading messages...
Received message from Partition: 0, Offset: 0, Key: Key-0, Value: Hello Kafka from Go! Message 0 at 2025-06-13T16:07:39+05:30
...
```

#### Manual Offset Consumer (`consumer.go - Manual Offset Management`)

This consumer explicitly commits offsets after successful processing. Messages where the last digit of the value is divisible by 3 will simulate a failure and will *not* have their offsets committed, leading to re-delivery on subsequent polls.

```bash
# Rename the consumer.go (Manual Offset Management)
mv consumer.go consumer_manual_offset.go
go run consumer_manual_offset.go
```

Observe how messages that fail processing are not committed and may be re-fetched.

```
Kafka Consumer for topic 'Ramayana', group 'new-consumer-group' started with MANUAL offset management. Reading messages...
Attempting to process message from Partition: 0, Offset: 0, Key: Key-0, Value: Hello Kafka from Go! Message 0 at 2025-06-13T16:07:39+05:30
SIMULATING PROCESSING FAILURE for message Partition: 0, Offset: 0
Attempting to process message from Partition: 1, Offset: 1, Key: Key-1, Value: Hello Kafka from Go! Message 1 at 2025-06-13T16:07:40+05:30
Finished processing message Partition: 1, Offset: 1
Successfully committed offset for message Partition: 1, Offset: 1
...
```

#### DLQ Consumer (`consumer.go - Robust Error Handling with DLQ`)

This consumer implements a retry mechanism and sends messages to a Dead Letter Queue (`Ramayana-dlq`) if they fail processing after `maxRetries` (3 attempts in this example). The simulated failure occurs if the last digit of the message value is even.

```bash
# Rename the consumer.go (Robust Error Handling with DLQ)
mv consumer.go consumer_dlq.go
go run consumer_dlq.go
```

You will see messages retrying and eventually being sent to the DLQ if they continuously fail:

```
Kafka Consumer for topic 'Ramayana', group 'youth-of-India' started with DLQ logic. Reading messages...
Attempt 1 for message from Partition: 0, Offset: 0, Key: Key-0, Value: Hello Kafka from Go! Message 0 at 2025-06-13T16:07:39+05:30
SIMULATING PROCESSING FAILURE for message Partition: 0, Offset: 0
Retrying message Partition: 0, Offset: 0. Current retries: 1
Attempt 2 for message from Partition: 0, Offset: 0, Key: Key-0, Value: Hello Kafka from Go! Message 0 at 2025-06-13T16:07:39+05:30
SIMULATING PROCESSING FAILURE for message Partition: 0, Offset: 0
Retrying message Partition: 0, Offset: 0. Current retries: 2
Attempt 3 for message from Partition: 0, Offset: 0, Key: Key-0, Value: Hello Kafka from Go! Message 0 at 2025-06-13T16:07:39+05:30
SIMULATING PROCESSING FAILURE for message Partition: 0, Offset: 0
MAX RETRIES REACHED for message Partition: 0, Offset: 0. Sending to DLQ.
Message Partition: 0, Offset: 0 sent to DLQ.
Committed offset for DLQ'd message Partition: 0, Offset: 0.
```

To see messages in the DLQ, you could run another simple consumer subscribed to the `Ramayana-dlq` topic.

#### Consumer Reading Headers (`consumer.go - Reading Message Headers`)

This consumer is similar to the DLQ consumer but explicitly demonstrates how to extract and print message headers.

```bash
# Rename the consumer.go (Reading Message Headers)
mv consumer.go consumer_headers.go
go run consumer_headers.go
```

You'll see the headers printed for each consumed message:

```
Attempt 1 for message | Partition: 0, Offset: 0, Key: Key-0, Value: Hello Kafka from Go! Message 0 at 2025-06-13T16:07:39+05:30
  Headers:
    - X-Trace-ID: trace-1718285259-0
    - X-Source-Service: go-producer-app
    - X-Timestamp: 1718285259123
SIMULATING PROCESSING FAILURE for message Partition: 0, Offset: 0
Retrying message Partition: 0, Offset: 0. Current retries: 1
...
```

---

## Code Explanation

### `docker-compose.yml`

This file defines the Kafka and Zookeeper services.

* **`zookeeper`**: The coordination service for Kafka. Exposed on port `2181`.
* **`kafka`**: The Kafka broker.
    * `depends_on`: Ensures Zookeeper starts before Kafka.
    * `ports`: Maps container port `9092` to host port `9092`, allowing Go applications to connect directly.
    * `KAFKA_ADVERTISED_LISTENERS`: Crucial for correct connectivity from outside the Docker network. `PLAINTEXT://localhost:9092` allows your Go application to connect from your host machine, while `PLAINTEXT_INTERNAL://kafka:29092` is for inter-broker communication within the Docker network.

### `admin_client.go`

This Go program uses the `github.com/segmentio/kafka-go` library to perform administrative operations, specifically **creating Kafka topics**.

* It connects to the Kafka broker using `kafka.TCP(adminBrokerAddress)`.
* A `context.WithTimeout` is used to prevent operations from hanging indefinitely.
* `kafka.CreateTopicsRequest` is used to define the topic name, number of partitions (`desiredPartitions = 3`), and replication factor (`desiredReplicas = 1`).
* It handles errors gracefully, checking for both general RPC errors and specific topic creation errors.

### `producer.go`

This program demonstrates how to **produce messages to a Kafka topic**.

* It creates a `kafka.Writer` instance configured with the broker address, topic, and a `Balancer` (here, `LeastBytes` to distribute messages by size).
* Messages are structured as `kafka.Message` with a `Key` and `Value`.
* **Message Headers**: The producer also demonstrates how to add custom headers to messages using `kafka.Header`. This is useful for adding metadata, trace IDs, or other contextual information that can be consumed by downstream applications.
* `writer.WriteMessages` sends the messages.
* A `context.Background()` is used for the write operation.

### `consumer.go` (Basic)

This is a fundamental Kafka consumer.

* It creates a `kafka.Reader` configured with broker address, topic, and a `GroupID`.
* **Consumer Groups**: Kafka uses consumer groups to distribute partitions among consumers. All consumers with the same `GroupID` cooperate to consume messages from a topic, ensuring each message is processed by only one consumer in the group.
* **Offset Management**: `CommitInterval: 1 * time.Second` configures the reader to automatically commit offsets to Kafka every second. This means Kafka will periodically save the consumer's progress.
* **Graceful Shutdown**: It uses `context.WithCancel` and `os.Signal` handling (`syscall.SIGINT`, `syscall.SIGTERM`) to ensure the consumer shuts down cleanly when the application receives a termination signal (e.g., Ctrl+C).
* `reader.FetchMessage` blocks until a message is available or the context is cancelled.

### `consumer.go` (Manual Offset Management)

This consumer builds upon the basic one by illustrating **manual offset commitment**.

* **`CommitInterval: 0`**: This crucial setting disables automatic offset commits.
* **`reader.CommitMessages(ctx, m)`**: This function is explicitly called *after* a message is successfully processed.
* **Simulated Failure**: It includes a simulated processing failure (if the last digit of the message value is divisible by 3). If processing fails, `CommitMessages` is *not* called, ensuring that Kafka will re-deliver this message on the next fetch, adhering to "at-least-once" delivery semantics.

### `consumer.go` (Robust Error Handling with DLQ)

This consumer demonstrates a **Dead Letter Queue (DLQ) pattern** for robust error handling.

* **`maxRetries`**: Defines the maximum number of attempts to process a message.
* **`retryCounts` map**: An in-memory map (for demonstration) tracks retry counts per message (identified by partition and offset). In a production system, this would typically be persisted or handled via message headers.
* **Simulated Failure**: If a message fails processing (last digit of value is even), its retry count is incremented.
* **DLQ Logic**: If `maxRetries` is reached, the message is sent to the `Ramayana-dlq` topic using a separate `kafka.Writer` (`dlqWriter`).
* **DLQ Message Headers**: A `X-DLQ-Reason` header is added to the DLQ message to indicate why it was moved.
* **Offset Commitment for DLQ'd messages**: Crucially, the offset of the original message *is committed* even after it's sent to the DLQ. This prevents the problematic message from indefinitely blocking the main consumer stream. The DLQ is then meant for separate monitoring and reprocessing.

### `consumer.go` (Reading Message Headers)

This consumer focuses on demonstrating **how to access and read message headers**.

* It's largely based on the DLQ consumer, but the key addition is the loop over `m.Headers`.
* `m.Headers` is a slice of `kafka.Header` structs, each containing a `Key` and `Value` (both `[]byte`).
* The example iterates through the headers and prints them, showing how to access specific header values.

---

## Cleanup

To stop and remove the Docker containers, run:

```bash
docker-compose down -v
```

The `-v` flag removes volumes, which means any Kafka data (topic offsets, messages) will be deleted.

---

## Troubleshooting

* **"failed to create topic: dial tcp 127.0.0.1:9092: connect: connection refused"**:
    * Ensure Docker Desktop is running.
    * Verify Kafka and Zookeeper containers are up and healthy (`docker-compose ps`).
    * Check your `KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml` to ensure `localhost:9092` is correctly configured.
* **"error creating topic 'Ramayana': kafka: topic already exists"**:
    * This is expected if you run `admin_client.go` multiple times without deleting the topic. You can safely ignore this or use `docker-compose down -v` to clear previous data.
* **Messages not appearing in consumer**:
    * Ensure the producer is running and actively sending messages.
    * Verify the topic name and broker address are consistent across producer and consumer.
    * Check the consumer group ID. If a message is consumed by another consumer in the *same* group, it won't be delivered to your current consumer. Try changing the `consumerGroup` in your consumer if you're experimenting with multiple instances.
    * For manual offset consumers, ensure offsets are being committed, or check the retry logic for messages not being committed.

---

## Contributing

Feel free to open issues or pull requests if you have suggestions, improvements, or bug fixes.

---

## License

This project is open-sourced under the MIT License. 
---