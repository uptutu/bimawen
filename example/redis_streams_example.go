// Redis Streams Consumer Groups Example
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	bimawen "local.git/libs/bimawen.git"
)

func main() {
	// Example 1: Basic Redis Streams Producer
	fmt.Println("=== Redis Streams Producer Example ===")
	producer, err := bimawen.NewProducer("redis-streams://localhost:6379/0")
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Send a message to a stream
	message := &bimawen.Message{
		ID:        "msg-1",
		Body:      []byte("Hello Redis Streams!"),
		Headers:   map[string]interface{}{"source": "example", "version": "1.0"},
		Timestamp: time.Now(),
		Priority:  1,
	}

	ctx := context.Background()
	err = producer.Send(ctx, "user-events", message)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
	} else {
		fmt.Println("Message sent successfully to stream 'user-events'")
	}

	// Example 2: Redis Streams Consumer with Consumer Group
	fmt.Println("\n=== Redis Streams Consumer Group Example ===")
	
	// Create consumer with circuit breaker
	cbConfig := bimawen.DefaultCircuitBreakerConfig()
	cbConfig.Enabled = true
	cbConfig.FailureThreshold = 3
	cbConfig.Timeout = 30 * time.Second

	consumerOptions := &bimawen.ConsumerOptions{
		URI:                  "redis-streams://localhost:6379/0",
		Concurrency:          2,
		AutoAck:              false,
		RetryConfig:          bimawen.DefaultRetryConfig(),
		CircuitBreakerConfig: cbConfig,
	}

	consumer, err := bimawen.NewConsumer("redis-streams://localhost:6379/0", consumerOptions)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Message handler
	handler := func(ctx context.Context, msg *bimawen.Message) error {
		fmt.Printf("Received message: ID=%s, Body=%s, Headers=%v\n", 
			msg.ID, string(msg.Body), msg.Headers)
		return nil // Return error to trigger retry/DLQ behavior
	}

	// Consume with consumer group
	consumeOptions := &bimawen.ConsumeOptions{
		ConsumerGroup: "user-service-group",
		ConsumerName:  "consumer-1",
		StartPosition: "earliest",
		MaxMessages:   10,
	}

	// Start consuming (this would run indefinitely in a real application)
	consumeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	fmt.Println("Starting consumer group consumption...")
	err = consumer.ConsumeWithOptions(consumeCtx, "user-events", handler, consumeOptions)
	if err != nil && err != context.DeadlineExceeded {
		log.Printf("Consumer error: %v", err)
	}

	// Example 3: Dead Letter Queue Configuration
	fmt.Println("\n=== Dead Letter Queue Configuration ===")
	dlqConfig := &bimawen.DeadLetterConfig{
		Enabled:             true,
		QueueName:           "failed-messages",
		ExchangeName:        "dlq-exchange",
		MaxRetries:          3,
		RetryStrategy:       bimawen.RetryStrategyExponential,
		BaseRetryDelay:      1 * time.Second,
		RetryMultiplier:     2.0,
		MaxRetryDelay:       30 * time.Second,
		AutoRequeue:         false,
		PreserveHeaders:     true,
		OnAlert: func(msg *bimawen.DeadLetterMessage) {
			fmt.Printf("ALERT: Message %s failed after %d attempts\n", msg.OriginalMessageID, msg.FailureCount)
		},
	}

	consumerWithDLQ := &bimawen.ConsumerOptions{
		URI:                "redis-streams://localhost:6379/0",
		Concurrency:        2,
		AutoAck:            false,
		RetryConfig:        bimawen.DefaultRetryConfig(),
		DeadLetterConfig:   dlqConfig,
	}

	fmt.Printf("DLQ Config: %+v\n", dlqConfig)
	fmt.Printf("Consumer Options: %+v\n", consumerWithDLQ)

	fmt.Println("\n=== Summary ===")
	fmt.Println("✅ Redis Streams producer and consumer group support")
	fmt.Println("✅ Circuit breaker pattern for fault tolerance") 
	fmt.Println("✅ Advanced dead letter queue with multiple retry strategies")
	fmt.Println("✅ Complete integration with existing bimawen architecture")
	fmt.Println("✅ URI-based driver selection: redis-streams://")
}