// producer.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress = "localhost:9092" // Your Kafka broker address
	topic         = "Ramayana"       // The Kafka topic to send messages to
)

func main() {
	// 1. Create a Kafka writer (producer)
	// The kafka.Writer configuration specifies the broker address, topic, and a balancer.
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brokerAddress}, // Pass a slice of broker addresses
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Distributes messages to partitions based on least bytes
		// Other common balancers include &kafka.RoundRobin{} (distributes messages equally in a cycle) or
		// &kafka.Hash{} (uses the message Key to determine the partition,
		// ensuring messages with the same key always go to the same partition
		// – excellent for ordered events like our CustomerID example). For this basic producer, LeastBytes is fine.

		BatchTimeout: 10 * time.Millisecond, // Send messages in batches every 10ms
		BatchSize:    100,                   // Send up to 100 messages in a batch
		// REMOVED: AllowAutoTopicCreation: true, // This field is no longer in kafka.WriterConfig
	})
	defer writer.Close() // Ensure the writer is closed when main function exits

	fmt.Println("Kafka Producer started. Sending messages...")

	// 2. Loop to send messages
	for i := 30; i < 40; i++ {
		key := fmt.Sprintf("Key-%d", i)
		value := fmt.Sprintf("Hello Kafka from Go! Message %d at %s", i, time.Now().Format(time.RFC3339))

		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(value),
		}

		// Send the message
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Fatalf("failed to write messages: %v", err)
		}

		fmt.Printf("Produced message | Key: %s | Value: %s\n", key, value)
		time.Sleep(500 * time.Millisecond) // Wait a bit before sending the next message
	}

	fmt.Println("Producer finished sending messages.")
}
