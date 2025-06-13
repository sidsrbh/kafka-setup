// admin_client.go
package main

import (
	"context" // Package for managing contexts, used for cancellation and timeouts
	"fmt"     // Package for formatted I/O (e.g., printing to console)
	"log"     // Package for logging messages (e.g., fatal errors)
	"time"    // Package for measuring and displaying time

	"github.com/segmentio/kafka-go" // Kafka client library for Go
)

const (
	adminBrokerAddress = "localhost:9092" // Address of the Kafka broker for admin operations
	adminTopic         = "Ramayana-dlq"   // The name of the Kafka topic to be created
	desiredPartitions  = 3                // The desired number of partitions for the new topic
	desiredReplicas    = 1                // The desired replication factor for the new topic (1 for a single broker setup)
)

func main() {
	// Create an admin client instance.
	// The kafka.Client uses the provided Addr for all its administrative operations.
	admClient := &kafka.Client{
		Addr: kafka.TCP(adminBrokerAddress), // Connects to the Kafka broker using TCP
	}

	// Create a context with a timeout. This ensures that the admin operation
	// (topic creation) will not hang indefinitely if the Kafka broker is unresponsive.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// 'defer cancel()' ensures that the context's resources are released
	// when the main function exits, regardless of whether it completes successfully or with an error.
	defer cancel()

	// Define the configuration for the topic(s) to be created.
	// This is a slice of kafka.TopicConfig, allowing for multiple topics to be configured.
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             adminTopic,        // Name of the topic
			NumPartitions:     desiredPartitions, // Number of partitions for the topic
			ReplicationFactor: desiredReplicas,   // Replication factor for the topic
		},
		//Add DLQ topic in the beginning for convenience
		// {
		// 	Topic:             adminTopic + "-dlq", // Name of the topic
		// 	NumPartitions:     desiredPartitions,   // Number of partitions for the topic
		// 	ReplicationFactor: desiredReplicas,     // Replication factor for the topic
		// },
	}

	// NEW: Create a CreateTopicsRequest struct. This struct holds the details
	// of the topics to be created, as required by the CreateTopics method.
	createReq := &kafka.CreateTopicsRequest{
		Topics: topicConfigs, // Assign the slice of topic configurations to the request
	}

	fmt.Printf("Creating topic '%s' with %d partitions and %d replicas...\n", adminTopic, desiredPartitions, desiredReplicas)

	// NEW: Pass the CreateTopicsRequest to the CreateTopics method of the admin client.
	// This sends the request to the Kafka broker to create the specified topic(s).
	resp, err := admClient.CreateTopics(ctx, createReq)
	if err != nil {
		// If there's a general error in the RPC call (e.g., network issue, broker unreachable),
		// log a fatal error and exit.
		log.Fatalf("failed to create topic: %v", err)
	}

	// CORRECTED: Check for errors reported within the response for each individual topic.
	// The 'resp.Errors' is a map where keys are topic names and values are errors
	// specific to the creation of that topic.
	for topicName, topicError := range resp.Errors { // Iterate over each entry in the errors map
		if topicError != nil {
			// If a specific topic creation had an error (e.g., topic already exists, invalid config),
			// log a fatal error for that topic and exit.
			log.Fatalf("error creating topic '%s': %v", topicName, topicError) // Access topic name and error directly from the map
		}
	}

	fmt.Printf("Topic '%s' created successfully!\n", adminTopic)
}
