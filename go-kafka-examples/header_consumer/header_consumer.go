// consumer.go (Reading Message Headers)
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv" // For using strconv.Atoi and strconv.FormatInt
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress = "localhost:9092"
	topic         = "Ramayana"
	dlqTopic      = "Ramayana-dlq"
	consumerGroup = "my-consumer-group"
	maxRetries    = 3
)

// In-memory map for retry counts (for demonstration purposes only)
var retryCounts = make(map[string]int)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

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

	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{brokerAddress},
		Topic:        dlqTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
	})
	defer dlqWriter.Close()

	fmt.Printf("Kafka Consumer for topic '%s', group '%s' started. Reading messages with headers...\n", topic, consumerGroup)

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
						return
					}
					log.Printf("Error fetching message: %v", err)
					time.Sleep(1 * time.Second)
					continue
				}

				msgIdentifier := fmt.Sprintf("%d-%d", m.Partition, m.Offset)
				currentRetryCount := retryCounts[msgIdentifier]

				fmt.Printf("\nAttempt %d for message | Partition: %d, Offset: %d, Key: %s, Value: %s\n",
					currentRetryCount+1, m.Partition, m.Offset, string(m.Key), string(m.Value))

				// --- NEW: Process Headers ---
				if len(m.Headers) > 0 {
					fmt.Println("  Headers:")
					for _, header := range m.Headers {
						fmt.Printf("    - %s: %s\n", header.Key, string(header.Value))
						// You can access specific headers like this:
						// if header.Key == "X-Trace-ID" {
						//    traceID := string(header.Value)
						//    // Use traceID for logging, etc.
						// }
					}
				}
				// --- End Header Processing ---

				// --- Simulate Processing Logic with Potential Failure (Same as before) ---
				processingSuccessful := true
				valStr := string(m.Value)
				if num, err := strconv.Atoi(valStr[len(valStr)-1:]); err == nil && num%2 == 0 {
					processingSuccessful = false
				}

				if !processingSuccessful {
					fmt.Printf("SIMULATING PROCESSING FAILURE for message Partition: %d, Offset: %d\n", m.Partition, m.Offset)
					currentRetryCount++
					retryCounts[msgIdentifier] = currentRetryCount

					if currentRetryCount >= maxRetries {
						fmt.Printf("MAX RETRIES REACHED for message Partition: %d, Offset: %d. Sending to DLQ.\n", m.Partition, m.Offset)

						// Add a DLQ-specific header when sending to DLQ
						dlqMsg := m
						dlqMsg.Headers = append(dlqMsg.Headers, kafka.Header{
							Key:   "X-DLQ-Reason",
							Value: []byte(fmt.Sprintf("Failed after %d attempts", maxRetries)),
						})

						dlqErr := dlqWriter.WriteMessages(ctx, dlqMsg)
						if dlqErr != nil {
							log.Printf("CRITICAL: Failed to send message to DLQ for Partition: %d, Offset: %d: %v", m.Partition, m.Offset, dlqErr)
						} else {
							fmt.Printf("Message Partition: %d, Offset: %d sent to DLQ.\n", m.Partition, m.Offset)
							delete(retryCounts, msgIdentifier)
						}
						err = reader.CommitMessages(ctx, m)
						if err != nil {
							log.Printf("CRITICAL: Error committing offset after DLQ for Partition: %d, Offset: %d: %v", m.Partition, m.Offset, err)
						} else {
							fmt.Printf("Committed offset for DLQ'd message Partition: %d, Offset: %d.\n", m.Partition, m.Offset)
						}
					} else {
						fmt.Printf("Retrying message Partition: %d, Offset: %d. Current retries: %d\n", m.Partition, m.Offset, currentRetryCount)
						time.Sleep(1 * time.Second)
					}
					continue
				}

				// --- Successful Processing ---
				fmt.Printf("Finished processing message Partition: %d, Offset: %d\n", m.Partition, m.Offset)
				delete(retryCounts, msgIdentifier)
				time.Sleep(500 * time.Millisecond)

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
