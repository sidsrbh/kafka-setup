// consumer.go (Manual Offset Management)
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv" // Added for integer to string conversion in simulation
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress = "localhost:9092"
	topic         = "Ramayana"
	consumerGroup = "new-consumer-group"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		GroupID:  consumerGroup,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  1 * time.Second,

		// *** IMPORTANT CHANGE FOR MANUAL COMMITS ***
		// Disable automatic commits by setting CommitInterval to 0.
		CommitInterval: 0, // No auto-commit
		// HeartbeatInterval: 3 * time.Second, // Still good to have for group coordination
		// SessionTimeout: 10 * time.Second,   // Still good to have for group coordination
	})
	defer reader.Close() // Ensure the reader is closed

	fmt.Printf("Kafka Consumer for topic '%s', group '%s' started with MANUAL offset management. Reading messages...\n", topic, consumerGroup)

	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Context cancelled. Shutting down consumer goroutine.")
				return
			default:
				m, err := reader.FetchMessage(ctx) // Fetch one message
				if err != nil {
					if ctx.Err() != nil {
						return // Context cancelled, exit gracefully
					}
					log.Printf("Error fetching message: %v", err)
					time.Sleep(1 * time.Second) // Wait a bit before retrying to avoid tight loops on persistent errors.
					continue
				}

				// --- Start Message Processing (Simulated Business Logic) ---
				fmt.Printf("Attempting to process message from Partition: %d, Offset: %d, Key: %s, Value: %s\n",
					m.Partition, m.Offset, string(m.Key), string(m.Value))

				// Simulate a conditional processing failure.
				// For example, if the message value can be converted to a number
				// and that number is divisible by 3, we simulate a failure.
				valStr := string(m.Value)
				isSimulatedFailure := false
				if num, err := strconv.Atoi(valStr[len(valStr)-1:]); err == nil && num%3 == 0 {
					isSimulatedFailure = true
				}

				if isSimulatedFailure {
					fmt.Printf("SIMULATING PROCESSING FAILURE for message Partition: %d, Offset: %d\n", m.Partition, m.Offset)
					// In a real scenario, you might log the error, send to a Dead Letter Queue, etc.
					// IMPORTANT: We DO NOT commit the offset on failure.
					// This means the message will be re-delivered on the next poll/restart.
					time.Sleep(2 * time.Second) // Simulate time spent trying to process
					continue                    // Skip committing for this message, move to next fetch
				}

				// If processing is successful:
				time.Sleep(500 * time.Millisecond) // Simulate successful work duration
				fmt.Printf("Finished processing message Partition: %d, Offset: %d\n", m.Partition, m.Offset)
				// --- End Message Processing ---

				// *** MANUAL COMMIT AFTER SUCCESSFUL PROCESSING ***
				// Commit the offset only AFTER the message has been fully processed
				// and all its side effects (e.g., database writes, API calls) are complete.
				err = reader.CommitMessages(ctx, m)
				if err != nil {
					// IMPORTANT: Handle commit errors. If the commit itself fails (e.g., network issue to Kafka),
					// the message will still be re-processed on next restart (at-least-once guarantee).
					// In a real system, you might retry the commit a few times or log a critical alert.
					log.Printf("CRITICAL: Error committing offset for message Partition: %d, Offset: %d: %v",
						m.Partition, m.Offset, err)
				} else {
					fmt.Printf("Successfully committed offset for message Partition: %d, Offset: %d\n", m.Partition, m.Offset)
				}
			}
		}
	}()

	// Wait for termination signal to gracefully shut down the application
	<-sigChan
	fmt.Println("Termination signal received. Shutting down...")
	cancel() // Signal context cancellation

	// Give a brief moment for the consumer goroutine to respond to cancellation and clean up.
	// The `defer reader.Close()` will handle flushing any pending operations and closing connections.
	time.Sleep(2 * time.Second)
	fmt.Println("Consumer shut down gracefully.")
}
