// consumer.go (Robust Error Handling with DLQ)
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress = "localhost:9092"
	topic         = "Ramayana"
	dlqTopic      = "Ramayana-dlq" // New topic for Dead Letter Queue
	consumerGroup = "youth-of-India"
	maxRetries    = 3 // Max attempts to process a message before sending to DLQ
)

// In-memory map to track retry counts for demonstration.
// In production, this should be persistent (e.g., Redis, DB) or part of message headers.
var retryCounts = make(map[string]int) // Key: Partition-Offset, Value: RetryCount

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Consumer Reader setup
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokerAddress},
		Topic:          topic,
		GroupID:        consumerGroup,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		MaxWait:        1 * time.Second,
		CommitInterval: 0, // Manual commits
	})
	defer reader.Close()

	// DLQ Producer Writer setup (used *by the consumer* to send to DLQ)
	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{brokerAddress},
		Topic:        dlqTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
		// DLQ topic should typically be pre-created, but for dev, we can auto-create.
	})
	defer dlqWriter.Close()

	fmt.Printf("Kafka Consumer for topic '%s', group '%s' started with DLQ logic. Reading messages...\n", topic, consumerGroup)

	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Context cancelled. Shutting down consumer goroutine.")
				return
			default:
				m, err := reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return // Context cancelled
					}
					log.Printf("Error fetching message: %v", err)
					time.Sleep(1 * time.Second)
					continue
				}

				msgIdentifier := fmt.Sprintf("%d-%d", m.Partition, m.Offset)
				currentRetryCount := retryCounts[msgIdentifier]

				fmt.Printf("Attempt %d for message from Partition: %d, Offset: %d, Key: %s, Value: %s\n",
					currentRetryCount+1, m.Partition, m.Offset, string(m.Key), string(m.Value))

				// --- Simulate Processing Logic with Potential Failure ---
				processingSuccessful := true
				valStr := string(m.Value)
				// Simulate failure if the last digit of the message value is divisible by 2
				if num, err := strconv.Atoi(valStr[len(valStr)-1:]); err == nil && num%2 == 0 {
					processingSuccessful = false
				}

				if !processingSuccessful {
					fmt.Printf("SIMULATING PROCESSING FAILURE for message Partition: %d, Offset: %d\n", m.Partition, m.Offset)
					currentRetryCount++
					retryCounts[msgIdentifier] = currentRetryCount

					if currentRetryCount >= maxRetries {
						// Max retries reached, send to DLQ
						fmt.Printf("MAX RETRIES REACHED for message Partition: %d, Offset: %d. Sending to DLQ.\n", m.Partition, m.Offset)
						dlqMsg := m // The original message is the DLQ message

						// Optionally add a header to the DLQ message with error details
						dlqMsg.Headers = append(dlqMsg.Headers, kafka.Header{
							Key:   "X-DLQ-Reason",
							Value: []byte(fmt.Sprintf("Failed after %d attempts", maxRetries)),
						})

						dlqErr := dlqWriter.WriteMessages(ctx, dlqMsg)
						if dlqErr != nil {
							log.Printf("CRITICAL: Failed to send message to DLQ for Partition: %d, Offset: %d: %v", m.Partition, m.Offset, dlqErr)
							// This is a serious error. If you can't even DLQ, you might need
							// to halt processing or implement an emergency local persistent queue.
						} else {
							fmt.Printf("Message Partition: %d, Offset: %d sent to DLQ.\n", m.Partition, m.Offset)
							delete(retryCounts, msgIdentifier) // Clean up retry count
						}
						// IMPORTANT: Commit the offset of the *original* message even if DLQ fails,
						// to prevent it from blocking the main stream indefinitely.
						// The DLQ itself should be monitored and re-processed.
						err = reader.CommitMessages(ctx, m)
						if err != nil {
							log.Printf("CRITICAL: Error committing offset after DLQ for Partition: %d, Offset: %d: %v", m.Partition, m.Offset, err)
						} else {
							fmt.Printf("Committed offset for DLQ'd message Partition: %d, Offset: %d.\n", m.Partition, m.Offset)
						}
					} else {
						// Not max retries yet, just log and continue (message will be re-fetched)
						fmt.Printf("Retrying message Partition: %d, Offset: %d. Current retries: %d\n", m.Partition, m.Offset, currentRetryCount)
						time.Sleep(1 * time.Second) // Small backoff before next retry
					}
					continue // Go fetch next message (or retry this one if Kafka redelivers)
				}

				// --- Successful Processing ---
				fmt.Printf("Finished processing message Partition: %d, Offset: %d\n", m.Partition, m.Offset)
				delete(retryCounts, msgIdentifier) // Clean up retry count on success
				time.Sleep(500 * time.Millisecond) // Simulate successful work duration

				// Manual Commit on Success
				err = reader.CommitMessages(ctx, m)
				if err != nil {
					log.Printf("CRITICAL: Error committing offset for message Partition: %d, Offset: %d: %v",
						m.Partition, m.Offset, err)
				} else {
					fmt.Printf("Successfully committed offset for message Partition: %d, Offset: %d\n", m.Partition, m.Offset)
				}
			}
		}
	}()

	<-sigChan
	fmt.Println("Termination signal received. Shutting down...")
	cancel()

	time.Sleep(2 * time.Second)
	fmt.Println("Consumer shut down gracefully.")
}
