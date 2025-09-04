package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"local.git/libs/bimawen.git"
)

func main() {
	fmt.Println("=== RabbitMQ Exchange Configuration Example ===")

	// Test RabbitMQ exchange configuration
	testRabbitMQExchangeConfiguration()

	fmt.Println("\n=== Exchange Configuration Example Complete ===")
}

func testRabbitMQExchangeConfiguration() {
	// Create producer options with RabbitMQ-specific exchange configuration
	opts := bimawen.DefaultProducerOptions()
	opts.URI = "amqp://guest:guest@localhost:5672/"
	
	// Configure RabbitMQ-specific options
	opts.DriverOptions = make(map[string]interface{})
	opts.DriverOptions[bimawen.RabbitMQExchange] = "my-direct-exchange"  // Specify exchange name
	opts.DriverOptions[bimawen.RabbitMQMandatory] = false                // Optional: mandatory delivery
	opts.DriverOptions[bimawen.RabbitMQImmediate] = false                // Optional: immediate delivery

	// Try to create producer
	producer, err := bimawen.NewProducer("amqp://guest:guest@localhost:5672/", opts)
	if err != nil {
		fmt.Printf("RabbitMQ not available: %v (this is normal if RabbitMQ is not running)\n", err)
		fmt.Println("\nHow to test with RabbitMQ running:")
		fmt.Println("1. Start RabbitMQ: docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management")
		fmt.Println("2. Create exchange: Use RabbitMQ Management UI at http://localhost:15672 (guest/guest)")
		fmt.Println("3. Create exchange named 'my-direct-exchange' of type 'direct'")
		fmt.Println("4. Run this example again")
		return
	}
	defer producer.Close()

	ctx := context.Background()

	// Create a test message
	message := &bimawen.Message{
		ID:        "exchange-test-001",
		Body:      []byte(`{"message": "Hello from custom exchange!", "timestamp": "` + time.Now().Format(time.RFC3339) + `"}`),
		Headers:   map[string]interface{}{"source": "exchange-example"},
		Timestamp: time.Now(),
		Priority:  1,
	}

	fmt.Printf("✅ Producer created with exchange configuration: %s\n", opts.DriverOptions[bimawen.RabbitMQExchange])
	
	// Send message to specific routing key
	// The message will be published to "my-direct-exchange" with routing key "order.created"
	err = producer.Send(ctx, "order.created", message)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		fmt.Println("\nPossible reasons:")
		fmt.Println("- Exchange 'my-direct-exchange' doesn't exist")
		fmt.Println("- Exchange type doesn't match routing key pattern")
		fmt.Println("- Connection or permissions issue")
		return
	}

	fmt.Println("✅ Successfully sent message to exchange 'my-direct-exchange' with routing key 'order.created'")

	// Send batch of messages with different routing keys
	fmt.Println("\n📦 Sending batch of messages with different routing keys...")
	
	messages := []*bimawen.Message{
		{
			ID:        "batch-order-001",
			Body:      []byte(`{"order_id": "12345", "status": "created"}`),
			Headers:   map[string]interface{}{"type": "order_created"},
			Timestamp: time.Now(),
		},
		{
			ID:        "batch-order-002", 
			Body:      []byte(`{"order_id": "12346", "status": "paid"}`),
			Headers:   map[string]interface{}{"type": "order_paid"},
			Timestamp: time.Now(),
		},
		{
			ID:        "batch-user-001",
			Body:      []byte(`{"user_id": "user123", "action": "login"}`),
			Headers:   map[string]interface{}{"type": "user_action"},
			Timestamp: time.Now(),
		},
	}

	// All messages in batch will be sent to the same exchange "my-direct-exchange"
	// but with different routing keys
	routingKeys := []string{"order.created", "order.paid", "user.login"}
	
	for i, msg := range messages {
		err = producer.Send(ctx, routingKeys[i], msg)
		if err != nil {
			log.Printf("Failed to send message %d: %v", i+1, err)
		} else {
			fmt.Printf("   ✅ Sent message to routing key: %s\n", routingKeys[i])
		}
	}

	fmt.Println("✅ Batch sending complete")

	// Demonstrate different exchange configurations
	fmt.Println("\n🔧 Demonstrating different exchange configurations...")

	// Example 1: Topic exchange configuration
	topicOpts := bimawen.DefaultProducerOptions()
	topicOpts.URI = "amqp://guest:guest@localhost:5672/"
	topicOpts.DriverOptions = map[string]interface{}{
		bimawen.RabbitMQExchange: "my-topic-exchange",
	}

	fmt.Printf("Configuration 1: Topic Exchange = %s\n", topicOpts.DriverOptions[bimawen.RabbitMQExchange])

	// Example 2: Fanout exchange configuration  
	fanoutOpts := bimawen.DefaultProducerOptions()
	fanoutOpts.URI = "amqp://guest:guest@localhost:5672/"
	fanoutOpts.DriverOptions = map[string]interface{}{
		bimawen.RabbitMQExchange: "my-fanout-exchange",
		bimawen.RabbitMQMandatory: true, // Require at least one queue bound
	}

	fmt.Printf("Configuration 2: Fanout Exchange = %s (mandatory = %v)\n", 
		fanoutOpts.DriverOptions[bimawen.RabbitMQExchange],
		fanoutOpts.DriverOptions[bimawen.RabbitMQMandatory])

	// Example 3: Default exchange (empty string)
	defaultOpts := bimawen.DefaultProducerOptions()
	defaultOpts.URI = "amqp://guest:guest@localhost:5672/"
	defaultOpts.DriverOptions = map[string]interface{}{
		bimawen.RabbitMQExchange: "", // Default exchange
	}

	fmt.Printf("Configuration 3: Default Exchange (empty string) = '%s'\n", 
		defaultOpts.DriverOptions[bimawen.RabbitMQExchange])

	fmt.Println("\n💡 Usage Tips:")
	fmt.Println("- Direct Exchange: Use specific routing keys like 'order.created', 'user.login'")
	fmt.Println("- Topic Exchange: Use pattern routing keys like 'order.*', 'user.*.action'") 
	fmt.Println("- Fanout Exchange: Routing key is ignored, all bound queues receive messages")
	fmt.Println("- Default Exchange: Routing key must match queue name exactly")
	fmt.Println("- Headers Exchange: Use message headers for routing (not implemented in this example)")
}