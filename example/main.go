package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"local.git/libs/bimawen.git"
)

func main() {
	// Example 1: RabbitMQ Producer and Consumer
	fmt.Println("=== RabbitMQ Example ===")
	rabbitMQExample()

	fmt.Println("\n=== Redis Example ===")
	redisExample()
}

func rabbitMQExample() {
	ctx := context.Background()
	
	// Create producer
	producer, err := bimawen.NewProducer("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Printf("Failed to create RabbitMQ producer: %v", err)
		return
	}
	defer producer.Close()

	// Create consumer
	consumer, err := bimawen.NewConsumer("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Printf("Failed to create RabbitMQ consumer: %v", err)
		return
	}
	defer consumer.Close()

	// Send a message
	message := &bimawen.Message{
		ID:        "msg-001",
		Body:      []byte(`{"order_id": "12345", "amount": 100.50}`),
		Headers:   map[string]interface{}{"source": "order-service"},
		Timestamp: time.Now(),
		Priority:  1,
	}

	fmt.Println("Sending message to RabbitMQ...")
	err = producer.Send(ctx, "orders", message)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		return
	}
	fmt.Println("Message sent successfully!")

	// Send message asynchronously
	fmt.Println("Sending async message to RabbitMQ...")
	asyncMessage := &bimawen.Message{
		ID:        "msg-002",
		Body:      []byte(`{"order_id": "12346", "amount": 200.75}`),
		Headers:   map[string]interface{}{"source": "order-service"},
		Timestamp: time.Now(),
		Priority:  2,
	}
	
	errChan := producer.SendAsync(ctx, "orders", asyncMessage)
	if err := <-errChan; err != nil {
		log.Printf("Failed to send async message: %v", err)
		return
	}
	fmt.Println("Async message sent successfully!")

	// Consume messages
	fmt.Println("Starting to consume messages from RabbitMQ...")
	consumeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = consumer.Consume(consumeCtx, "orders", func(ctx context.Context, msg *bimawen.Message) error {
		fmt.Printf("Received message: ID=%s, Body=%s, Headers=%v\n", 
			msg.ID, string(msg.Body), msg.Headers)
		return nil
	})

	if err != nil && err != context.DeadlineExceeded {
		log.Printf("Consumer error: %v", err)
	}
}

func redisExample() {
	ctx := context.Background()
	
	// Create producer
	producer, err := bimawen.NewProducer("redis://localhost:6379/0")
	if err != nil {
		log.Printf("Failed to create Redis producer: %v", err)
		return
	}
	defer producer.Close()

	// Create consumer
	consumer, err := bimawen.NewConsumer("redis://localhost:6379/0")
	if err != nil {
		log.Printf("Failed to create Redis consumer: %v", err)
		return
	}
	defer consumer.Close()

	// Send a message
	message := &bimawen.Message{
		ID:        "redis-msg-001",
		Body:      []byte(`{"user_id": "user123", "action": "login"}`),
		Headers:   map[string]interface{}{"ip": "192.168.1.1"},
		Timestamp: time.Now(),
		Priority:  1,
	}

	fmt.Println("Sending message to Redis...")
	err = producer.Send(ctx, "user-events", message)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		return
	}
	fmt.Println("Message sent successfully!")

	// Send delayed message
	delayedMessage := &bimawen.Message{
		ID:        "redis-msg-002",
		Body:      []byte(`{"user_id": "user123", "action": "reminder"}`),
		Headers:   map[string]interface{}{"type": "delayed"},
		Timestamp: time.Now(),
		Priority:  1,
		Delay:     5 * time.Second, // Delay for 5 seconds
	}

	fmt.Println("Sending delayed message to Redis...")
	err = producer.Send(ctx, "user-events", delayedMessage)
	if err != nil {
		log.Printf("Failed to send delayed message: %v", err)
		return
	}
	fmt.Println("Delayed message sent successfully!")

	// Send batch messages
	batchMessages := []*bimawen.Message{
		{
			ID:        "batch-msg-001",
			Body:      []byte(`{"batch": 1}`),
			Timestamp: time.Now(),
		},
		{
			ID:        "batch-msg-002",
			Body:      []byte(`{"batch": 2}`),
			Timestamp: time.Now(),
		},
		{
			ID:        "batch-msg-003",
			Body:      []byte(`{"batch": 3}`),
			Timestamp: time.Now(),
		},
	}

	fmt.Println("Sending batch messages to Redis...")
	err = producer.SendBatch(ctx, "batch-events", batchMessages)
	if err != nil {
		log.Printf("Failed to send batch messages: %v", err)
		return
	}
	fmt.Println("Batch messages sent successfully!")

	// Consume messages
	fmt.Println("Starting to consume messages from Redis...")
	consumeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	err = consumer.Consume(consumeCtx, "user-events", func(ctx context.Context, msg *bimawen.Message) error {
		fmt.Printf("Received message: ID=%s, Body=%s, Headers=%v\n", 
			msg.ID, string(msg.Body), msg.Headers)
		return nil
	})

	if err != nil && err != context.DeadlineExceeded {
		log.Printf("Consumer error: %v", err)
	}

	// Also consume batch messages
	fmt.Println("Starting to consume batch messages from Redis...")
	batchCtx, batchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer batchCancel()

	err = consumer.Consume(batchCtx, "batch-events", func(ctx context.Context, msg *bimawen.Message) error {
		fmt.Printf("Received batch message: ID=%s, Body=%s\n", 
			msg.ID, string(msg.Body))
		return nil
	})

	if err != nil && err != context.DeadlineExceeded {
		log.Printf("Batch consumer error: %v", err)
	}
}