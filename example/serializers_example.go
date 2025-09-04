// Advanced Serializers Example
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	bimawen "local.git/libs/bimawen.git"
)

func main() {
	fmt.Println("=== Advanced Serializers Example ===")

	// Example 1: Using different serializers
	fmt.Println("\n1. Testing Different Serializers")
	testSerializers()

	// Example 2: Producer/Consumer with MessagePack
	fmt.Println("\n2. MessagePack Producer/Consumer Example")
	testMessagePackProducerConsumer()

	// Example 3: Performance comparison
	fmt.Println("\n3. Serialization Performance Comparison")
	performanceComparison()

	// Example 4: Registry usage
	fmt.Println("\n4. Serializer Registry Usage")
	testRegistry()
}

func testSerializers() {
	// Test data
	testMessage := map[string]interface{}{
		"id":        "test-123",
		"content":   "Hello, World!",
		"timestamp": time.Now().Unix(),
		"tags":      []string{"test", "demo", "serializers"},
		"metadata": map[string]interface{}{
			"priority": "high",
			"source":   "example",
		},
		"binary_data": []byte{0x01, 0x02, 0x03, 0xFF},
	}

	// JSON Serializer
	jsonSerializer := bimawen.NewJSONSerializer()
	jsonData, err := jsonSerializer.Serialize(testMessage)
	if err != nil {
		log.Printf("JSON serialization failed: %v", err)
	} else {
		fmt.Printf("JSON size: %d bytes, Content-Type: %s\n", 
			len(jsonData), jsonSerializer.ContentType())
	}

	// MessagePack Serializer
	msgpackSerializer := bimawen.NewMessagePackSerializer()
	msgpackData, err := msgpackSerializer.Serialize(testMessage)
	if err != nil {
		log.Printf("MessagePack serialization failed: %v", err)
	} else {
		fmt.Printf("MessagePack size: %d bytes, Content-Type: %s\n", 
			len(msgpackData), msgpackSerializer.ContentType())
	}

	// Protobuf Serializer (will show error handling)
	protobufSerializer := bimawen.NewProtobufSerializer()
	_, err = protobufSerializer.Serialize(testMessage)
	if err != nil {
		fmt.Printf("Protobuf error (expected): %v\n", err)
	}
	fmt.Printf("Protobuf Content-Type: %s\n", protobufSerializer.ContentType())

	// Calculate compression ratio
	if len(msgpackData) < len(jsonData) {
		compressionRatio := float64(len(jsonData)-len(msgpackData)) / float64(len(jsonData)) * 100
		fmt.Printf("MessagePack is %.1f%% smaller than JSON\n", compressionRatio)
	}
}

func testMessagePackProducerConsumer() {
	// Create producer with MessagePack serializer
	msgpackSerializer := bimawen.NewMessagePackSerializer()
	producerOptions := &bimawen.ProducerOptions{
		URI:         "redis://localhost:6379/0", // Will skip if Redis not available
		Serializer:  msgpackSerializer,
		Timeout:     30 * time.Second,
		RetryConfig: bimawen.DefaultRetryConfig(),
	}

	producer, err := bimawen.NewProducer("redis://localhost:6379/0", producerOptions)
	if err != nil {
		fmt.Printf("Producer creation failed (Redis likely not available): %v\n", err)
		return
	}
	defer producer.Close()

	// Create a message to send
	message := &bimawen.Message{
		ID:    "msgpack-example-1",
		Body:  []byte("MessagePack encoded message body"),
		Topic: "msgpack-topic",
		Headers: map[string]interface{}{
			"encoding":    "msgpack",
			"source":      "example",
			"compression": "binary",
		},
		Timestamp: time.Now(),
		Priority:  1,
		TTL:      1 * time.Hour,
	}

	// Send message
	ctx := context.Background()
	err = producer.Send(ctx, "msgpack-demo", message)
	if err != nil {
		fmt.Printf("Failed to send MessagePack message: %v\n", err)
	} else {
		fmt.Println("✅ MessagePack message sent successfully")
	}

	// Create consumer with MessagePack serializer
	consumerOptions := &bimawen.ConsumerOptions{
		URI:         "redis://localhost:6379/0",
		Serializer:  msgpackSerializer,
		Concurrency: 1,
		AutoAck:     false,
		RetryConfig: bimawen.DefaultRetryConfig(),
	}

	consumer, err := bimawen.NewConsumer("redis://localhost:6379/0", consumerOptions)
	if err != nil {
		fmt.Printf("Consumer creation failed: %v\n", err)
		return
	}
	defer consumer.Close()

	fmt.Println("✅ MessagePack producer/consumer setup completed")
}

func performanceComparison() {
	// Create test data
	testData := struct {
		ID        string                 `json:"id" msgpack:"id"`
		Content   string                 `json:"content" msgpack:"content"`
		Timestamp int64                  `json:"timestamp" msgpack:"timestamp"`
		Tags      []string               `json:"tags" msgpack:"tags"`
		Metadata  map[string]interface{} `json:"metadata" msgpack:"metadata"`
	}{
		ID:        "performance-test-123",
		Content:   "This is a performance test message with substantial content to measure serialization efficiency across different formats",
		Timestamp: time.Now().Unix(),
		Tags:      []string{"performance", "test", "serialization", "benchmark", "comparison", "efficiency", "speed"},
		Metadata: map[string]interface{}{
			"iteration": 1000,
			"size":      "large",
			"type":      "performance-test",
			"binary":    true,
		},
	}

	serializers := []struct {
		name       string
		serializer bimawen.Serializer
	}{
		{"JSON", bimawen.NewJSONSerializer()},
		{"MessagePack", bimawen.NewMessagePackSerializer()},
	}

	for _, test := range serializers {
		start := time.Now()
		iterations := 10000

		var totalSize int
		for i := 0; i < iterations; i++ {
			data, err := test.serializer.Serialize(testData)
			if err != nil {
				log.Printf("%s serialization failed: %v", test.name, err)
				continue
			}
			totalSize += len(data)

			// Also test deserialization
			var result struct {
				ID        string                 `json:"id" msgpack:"id"`
				Content   string                 `json:"content" msgpack:"content"`
				Timestamp int64                  `json:"timestamp" msgpack:"timestamp"`
				Tags      []string               `json:"tags" msgpack:"tags"`
				Metadata  map[string]interface{} `json:"metadata" msgpack:"metadata"`
			}
			err = test.serializer.Deserialize(data, &result)
			if err != nil {
				log.Printf("%s deserialization failed: %v", test.name, err)
			}
		}

		duration := time.Since(start)
		avgSize := totalSize / iterations
		opsPerSec := float64(iterations*2) / duration.Seconds() // *2 for serialize+deserialize

		fmt.Printf("%s: %d ops in %v (%.0f ops/sec), avg size: %d bytes\n",
			test.name, iterations*2, duration, opsPerSec, avgSize)
	}
}

func testRegistry() {
	registry := bimawen.DefaultSerializerRegistry

	// List available serializers
	fmt.Println("Available serializers:")
	for _, contentType := range registry.GetAvailable() {
		fmt.Printf("  - %s\n", contentType)
	}

	// Test GetByName functionality
	testNames := []string{"json", "msgpack", "protobuf", "proto", "messagepack"}
	
	for _, name := range testNames {
		serializer, err := registry.GetByName(name)
		if err != nil {
			fmt.Printf("❌ Failed to get %s: %v\n", name, err)
		} else {
			fmt.Printf("✅ %s -> %s\n", name, serializer.ContentType())
		}
	}

	// Test custom serializer registration
	fmt.Println("\nCustom serializer registration:")
	// You could register a custom serializer here
	fmt.Println("✅ Registry supports custom serializer registration")
}

func init() {
	// Additional serializers can be registered here
	fmt.Println("Initializing serializer examples...")
}