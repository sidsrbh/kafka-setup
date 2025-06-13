// consumer.go
package main

import (
	"context"   // Used for managing request-scoped values, cancellation signals, and deadlines. Essential for graceful shutdowns.
	"fmt"       // Package for formatted I/O (e.g., printing messages to console).
	"log"       // Standard logging package, used here for fatal errors and general messages.
	"os"        // Provides access to operating system functionality, used here for signal handling.
	"os/signal" // Specific package for handling OS signals (e.g., Ctrl+C).
	"syscall"   // Provides low-level operating system primitives, used here for specific signal types.
	"time"      // Provides functionality for measuring and displaying time.

	"github.com/segmentio/kafka-go" // The Kafka client library for Go.
)

// Define constants for Kafka configuration.
const (
	brokerAddress = "localhost:9092" // The address of your Kafka broker. This should match the advertised listener from your docker-compose setup.
	topic         = "Ramayana"       // The Kafka topic from which to consume messages. This *must* match the topic name used by your producer.
	consumerGroup = "Kids-of-India"  // The ID of the consumer group this consumer belongs to.
	// All consumers with the same GroupID form a consumer group.
	// Kafka ensures that each message within a topic's partition is delivered to only one consumer within a group.
	// Different consumer groups can consume the same messages independently.
)

func main() {
	// 1. Context for Graceful Shutdown
	// Create a cancellable context. This context will be passed to Kafka operations (like FetchMessage).
	// When `cancel()` is called, `ctx.Done()` channel will close, signaling goroutines to stop.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure `cancel()` is called when the `main` function exits, releasing resources.

	// 2. Signal Handling for Graceful Shutdown
	// Create a channel to listen for OS signals.
	sigChan := make(chan os.Signal, 1)
	// Notify the `sigChan` channel when an interrupt (Ctrl+C) or termination signal is received.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM) // syscall.SIGINT for Ctrl+C, syscall.SIGTERM for standard termination.

	// 3. Create a Kafka Reader (Consumer)
	// `kafka.NewReader` creates a new Kafka consumer instance configured with specific settings.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress}, // A slice of Kafka broker addresses to connect to.
		Topic:   topic,                   // The Kafka topic to subscribe to.
		GroupID: consumerGroup,           // The consumer group ID this reader belongs to. Essential for Kafka's load balancing and offset management.

		// Consumer Fetching Configuration:
		// These settings control how the consumer fetches messages from Kafka brokers.
		MinBytes: 10e3,            // Minimum amount of data (in bytes) to fetch in a single request (e.g., 10KB). Kafka will wait until it has at least this much data.
		MaxBytes: 10e6,            // Maximum amount of data (in bytes) to fetch in a single request (e.g., 10MB).
		MaxWait:  1 * time.Second, // Maximum time the consumer will wait for `MinBytes` to be available before returning data (e.g., 1 second). This helps balance latency and throughput.

		// Offset Management Configuration:
		// Offsets track the consumer's position in a topic's partition.
		CommitInterval: 1 * time.Second, // Automatically commit offsets to Kafka every 1 second.
		// This means Kafka will periodically record how far the consumer has processed messages.
		// If the consumer restarts, it will resume from the last committed offset, ensuring at-least-once delivery.
		// If this is 0, no auto-commits occur, and you must manually commit offsets (e.g., after processing a batch).

		// Optional Advanced Settings:
		// HeartbeatInterval: 3 * time.Second, // How often the consumer sends heartbeats to the group coordinator to indicate it's alive.
		// SessionTimeout: 10 * time.Second,   // How long the group coordinator will wait for a heartbeat before considering the consumer dead and triggering a rebalance.
		// IsolationLevel: kafka.ReadCommitted, // For transactional reads. Ensures only committed messages are read.
	})
	defer reader.Close() // Ensure the reader is closed cleanly when `main` function exits. This flushes any buffered messages and commits pending offsets.

	fmt.Printf("Kafka Consumer for topic '%s', group '%s' started. Reading messages...\n", topic, consumerGroup)

	// 4. Message Consumption Goroutine
	// Run the message fetching logic in a separate goroutine.
	// This allows the `main` function to concurrently wait for termination signals.
	go func() {
		for {
			select {
			case <-ctx.Done(): // Check if the context has been cancelled (e.g., by a signal).
				fmt.Println("Context cancelled. Shutting down consumer goroutine.")
				return // Exit the goroutine gracefully.
			default:
				// Fetch the next message from Kafka. This call blocks until a message is available
				// or the provided context is cancelled/times out.
				m, err := reader.FetchMessage(ctx)
				if err != nil {
					// Check if the error is due to context cancellation.
					if ctx.Err() != nil {
						return // If context cancelled, it's a graceful exit, no need to log as a fatal error.
					}
					// Log other fetching errors (e.g., network issues, broker unavailable).
					log.Printf("Error fetching message: %v", err)
					time.Sleep(1 * time.Second) // Wait a bit before retrying to avoid tight loops on persistent errors.
					continue                    // Continue to the next iteration to try fetching again.
				}

				// Print the details of the received message.
				// m.Partition: The partition number the message was read from.
				// m.Offset: The sequential ID of the message within that partition.
				// m.Key: The message key (useful for ordering guarantees and data partitioning).
				// m.Value: The actual payload of the message.
				fmt.Printf("Received message from Partition: %d, Offset: %d, Key: %s, Value: %s\n",
					m.Partition, m.Offset, string(m.Key), string(m.Value))

				// 5. Commit Offsets
				// Explicitly commit the offset of the currently processed message.
				// This tells Kafka that this message (and all prior messages in this partition)
				// have been successfully processed by *this consumer group*.
				// Although CommitInterval is set, explicit commits can be useful for
				// fine-grained control, e.g., after processing a batch of messages.
				err = reader.CommitMessages(ctx, m)
				if err != nil {
					// Log errors during offset commitment, as this is crucial for preventing message re-processing on restart.
					log.Printf("Error committing offset for message Partition: %d, Offset: %d: %v",
						m.Partition, m.Offset, err)
				}
			}
		}
	}()

	// 6. Main Goroutine Waits for Termination Signal
	// This blocks the `main` function until a signal is received on `sigChan`.
	<-sigChan
	fmt.Println("Termination signal received. Shutting down...")
	// Call cancel() to signal the message consumption goroutine to stop.
	cancel()

	// 7. Graceful Shutdown Period
	// Give the consumer goroutine a small amount of time to complete any pending tasks
	// (e.g., committing final offsets, flushing internal buffers) before `main` exits.
	time.Sleep(2 * time.Second)
	fmt.Println("Consumer shut down gracefully.")
}
