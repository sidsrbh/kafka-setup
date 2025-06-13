// producer.go (with Message Headers)
package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress = "localhost:9092" // Your Kafka broker address
	topic         = "Ramayana"       // The Kafka topic to send messages to
)

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{brokerAddress},
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
	})
	defer writer.Close()

	fmt.Println("Kafka Producer started. Sending messages with headers...")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("Key-%d", i)
		value := fmt.Sprintf("Hello Kafka from Go! Message %d at %s", i, time.Now().Format(time.RFC3339))

		// Define Headers
		headers := []kafka.Header{
			{Key: "X-Trace-ID", Value: []byte(fmt.Sprintf("trace-%d-%d", time.Now().Unix(), i))},
			{Key: "X-Source-Service", Value: []byte("go-producer-app")},
			{Key: "X-Timestamp", Value: []byte(strconv.FormatInt(time.Now().UnixMilli(), 10))}, // Example: timestamp as string
		}

		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(value),
			// Assign headers to the message
			Headers: headers,
		}

		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Fatalf("failed to write messages: %v", err)
		}

		fmt.Printf("Produced message | Key: %s | Value: %s | Headers: %v\n", key, value, headers)
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("Producer finished sending messages.")
}
