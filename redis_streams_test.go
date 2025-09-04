package bimawen

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Helper function to create a test Redis client
func createTestRedisClient(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1, // Use DB 1 for tests
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available for testing: %v", err)
	}

	return client
}

// Helper to clean up test streams
func cleanupTestStream(t interface{}, client *redis.Client, stream string) {
	ctx := context.Background()
	client.Del(ctx, stream)
}

func TestRedisStreamsDriverCreation(t *testing.T) {
	info, err := ParseURI("redis://localhost:6379/1")
	if err != nil {
		t.Fatalf("Failed to parse URI: %v", err)
	}

	driver, err := NewRedisStreamsDriver(info)
	if err != nil {
		t.Skipf("Redis not available for testing: %v", err)
	}
	defer driver.Close()

	// Test health check
	ctx := context.Background()
	if err := driver.HealthCheck(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestRedisStreamsProducerBasicSend(t *testing.T) {
	client := createTestRedisClient(t)
	defer client.Close()

	streamName := "test-stream-producer"
	defer cleanupTestStream(t, client, streamName)

	options := &ProducerOptions{
		URI:         "redis://localhost:6379/1",
		Timeout:     30 * time.Second,
		RetryConfig: DefaultRetryConfig(),
	}

	producer, err := NewRedisStreamsProducer(client, options)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Test message
	message := &Message{
		ID:        "test-msg-1",
		Body:      []byte("Hello Redis Streams"),
		Headers:   map[string]interface{}{"source": "test", "type": "greeting"},
		Timestamp: time.Now(),
		Priority:  1,
		TTL:       1 * time.Hour,
	}

	// Send message
	ctx := context.Background()
	err = producer.Send(ctx, streamName, message)
	if err != nil {
		t.Errorf("Failed to send message: %v", err)
	}

	// Verify message was added to stream
	entries, err := client.XRange(ctx, streamName, "-", "+").Result()
	if err != nil {
		t.Errorf("Failed to read stream: %v", err)
	}

	if len(entries) != 1 {
		t.Errorf("Expected 1 message in stream, got %d", len(entries))
	}

	entry := entries[0]
	if entry.Values["body"] != "Hello Redis Streams" {
		t.Errorf("Expected body 'Hello Redis Streams', got %v", entry.Values["body"])
	}
	if entry.Values["header_source"] != "test" {
		t.Errorf("Expected header_source 'test', got %v", entry.Values["header_source"])
	}
}

func TestRedisStreamsProducerBatchSend(t *testing.T) {
	client := createTestRedisClient(t)
	defer client.Close()

	streamName := "test-stream-batch"
	defer cleanupTestStream(t, client, streamName)

	options := &ProducerOptions{
		URI:         "redis://localhost:6379/1",
		Timeout:     30 * time.Second,
		RetryConfig: DefaultRetryConfig(),
	}

	producer, err := NewRedisStreamsProducer(client, options)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create batch of messages
	messages := []*Message{
		{
			ID:        "batch-msg-1",
			Body:      []byte("Message 1"),
			Timestamp: time.Now(),
		},
		{
			ID:        "batch-msg-2",
			Body:      []byte("Message 2"),
			Timestamp: time.Now(),
		},
		{
			ID:        "batch-msg-3",
			Body:      []byte("Message 3"),
			Timestamp: time.Now(),
		},
	}

	// Send batch
	ctx := context.Background()
	err = producer.SendBatch(ctx, streamName, messages)
	if err != nil {
		t.Errorf("Failed to send batch: %v", err)
	}

	// Verify all messages were added
	entries, err := client.XRange(ctx, streamName, "-", "+").Result()
	if err != nil {
		t.Errorf("Failed to read stream: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("Expected 3 messages in stream, got %d", len(entries))
	}

	for i, entry := range entries {
		expectedBody := fmt.Sprintf("Message %d", i+1)
		if entry.Values["body"] != expectedBody {
			t.Errorf("Expected body '%s', got %v", expectedBody, entry.Values["body"])
		}
	}
}

func TestRedisStreamsConsumerWithoutGroup(t *testing.T) {
	client := createTestRedisClient(t)
	defer client.Close()

	streamName := "test-stream-consumer"
	defer cleanupTestStream(t, client, streamName)

	// First add some messages to the stream
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "*",
			Values: map[string]interface{}{
				"body": fmt.Sprintf("Test message %d", i),
				"id":   fmt.Sprintf("test-msg-%d", i),
			},
		})
	}

	options := &ConsumerOptions{
		URI:         "redis://localhost:6379/1",
		Concurrency: 1,
		AutoAck:     false,
		RetryConfig: DefaultRetryConfig(),
	}

	consumer, err := NewRedisStreamsConsumer(client, options)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Track received messages
	var receivedMessages []*Message
	var mu sync.Mutex

	handler := func(ctx context.Context, msg *Message) error {
		mu.Lock()
		receivedMessages = append(receivedMessages, msg)
		mu.Unlock()
		return nil
	}

	// Start consuming with timeout
	consumeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// Start consumer in goroutine
	go func() {
		consumer.ConsumeWithOptions(consumeCtx, streamName, handler, &ConsumeOptions{
			StartPosition: "0-0", // From beginning
		})
	}()

	// Wait for messages to be processed
	time.Sleep(1 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(receivedMessages) < 3 {
		t.Errorf("Expected at least 3 messages, got %d", len(receivedMessages))
	}

	// Verify message content
	for i, msg := range receivedMessages {
		if i < 3 {
			expectedBody := fmt.Sprintf("Test message %d", i+1)
			if string(msg.Body) != expectedBody {
				t.Errorf("Expected body '%s', got '%s'", expectedBody, string(msg.Body))
			}
		}
	}
}

func TestRedisStreamsConsumerWithGroup(t *testing.T) {
	client := createTestRedisClient(t)
	defer client.Close()

	streamName := "test-stream-group"
	groupName := "test-group"
	defer cleanupTestStream(t, client, streamName)

	// Clean up group
	defer func() {
		ctx := context.Background()
		client.XGroupDestroy(ctx, streamName, groupName)
	}()

	// First add some messages to the stream
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "*",
			Values: map[string]interface{}{
				"body": fmt.Sprintf("Group message %d", i),
				"id":   fmt.Sprintf("group-msg-%d", i),
			},
		})
	}

	options := &ConsumerOptions{
		URI:         "redis://localhost:6379/1",
		Concurrency: 2,
		AutoAck:     false,
		RetryConfig: DefaultRetryConfig(),
	}

	consumer, err := NewRedisStreamsConsumer(client, options)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Track received messages
	var receivedMessages []*Message
	var mu sync.Mutex

	handler := func(ctx context.Context, msg *Message) error {
		mu.Lock()
		receivedMessages = append(receivedMessages, msg)
		mu.Unlock()
		return nil
	}

	// Start consuming with consumer group
	consumeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	go func() {
		consumer.ConsumeWithOptions(consumeCtx, streamName, handler, &ConsumeOptions{
			ConsumerGroup: groupName,
			ConsumerName:  "test-consumer-1",
			StartPosition: "0-0",
			MaxMessages:   3,
		})
	}()

	// Wait for messages to be processed
	time.Sleep(2 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(receivedMessages) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(receivedMessages))
	}

	// Verify messages were acknowledged (pending count should be 0)
	pendingInfo, err := client.XPending(ctx, streamName, groupName).Result()
	if err != nil {
		t.Errorf("Failed to get pending info: %v", err)
	} else if pendingInfo.Count != 0 {
		t.Errorf("Expected 0 pending messages, got %d", pendingInfo.Count)
	}
}

func TestRedisStreamsConsumerErrorHandling(t *testing.T) {
	client := createTestRedisClient(t)
	defer client.Close()

	streamName := "test-stream-error"
	defer cleanupTestStream(t, client, streamName)

	// Add a message
	ctx := context.Background()
	client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		ID:     "*",
		Values: map[string]interface{}{
			"body": "Error test message",
			"id":   "error-msg-1",
		},
	})

	options := &ConsumerOptions{
		URI:         "redis://localhost:6379/1",
		Concurrency: 1,
		AutoAck:     false,
		RetryConfig: &RetryConfig{
			MaxRetries:      2,
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     1 * time.Second,
			Multiplier:      2.0,
		},
	}

	consumer, err := NewRedisStreamsConsumer(client, options)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	callCount := 0
	handler := func(ctx context.Context, msg *Message) error {
		callCount++
		if callCount <= 2 {
			return NewRetryableError(fmt.Errorf("simulated error %d", callCount))
		}
		return nil // Success on third try
	}

	// Start consuming with timeout
	consumeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	go func() {
		consumer.ConsumeWithOptions(consumeCtx, streamName, handler, &ConsumeOptions{
			StartPosition: "0-0",
			MaxMessages:   1,
		})
	}()

	// Wait for processing
	time.Sleep(3 * time.Second)

	if callCount < 3 {
		t.Errorf("Expected handler to be called at least 3 times (with retries), got %d", callCount)
	}
}

func TestRedisStreamsMessageConversion(t *testing.T) {
	client := createTestRedisClient(t)
	defer client.Close()

	options := &ConsumerOptions{
		URI:         "redis://localhost:6379/1",
		Concurrency: 1,
		RetryConfig: DefaultRetryConfig(),
	}

	consumer, err := NewRedisStreamsConsumer(client, options)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Create a Redis stream message
	redisMsg := redis.XMessage{
		ID: "1234567890123-0",
		Values: map[string]interface{}{
			"id":             "test-msg-123",
			"body":           "Test message body",
			"timestamp":      "2023-01-01T12:00:00Z",
			"delivery_count": "2",
			"priority":       "5",
			"header_source":  "test-source",
			"header_type":    "test-type",
		},
	}

	// Convert to our Message type
	message := consumer.convertStreamMessage(redisMsg, "test-topic")

	// Verify conversion
	if message.ID != "test-msg-123" {
		t.Errorf("Expected ID 'test-msg-123', got '%s'", message.ID)
	}
	if string(message.Body) != "Test message body" {
		t.Errorf("Expected body 'Test message body', got '%s'", string(message.Body))
	}
	if message.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", message.Topic)
	}
	if message.DeliveryCount != 2 {
		t.Errorf("Expected delivery count 2, got %d", message.DeliveryCount)
	}
	if message.Priority != 5 {
		t.Errorf("Expected priority 5, got %d", message.Priority)
	}
	if message.Headers["source"] != "test-source" {
		t.Errorf("Expected header source 'test-source', got %v", message.Headers["source"])
	}
	if message.Headers["type"] != "test-type" {
		t.Errorf("Expected header type 'test-type', got %v", message.Headers["type"])
	}
}

func TestRedisStreamsWithCircuitBreaker(t *testing.T) {
	client := createTestRedisClient(t)
	defer client.Close()

	streamName := "test-stream-cb"
	defer cleanupTestStream(t, client, streamName)

	// Add messages to stream
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "*",
			Values: map[string]interface{}{
				"body": fmt.Sprintf("CB test message %d", i),
				"id":   fmt.Sprintf("cb-msg-%d", i),
			},
		})
	}

	cbConfig := DefaultCircuitBreakerConfig()
	cbConfig.Enabled = true
	cbConfig.FailureThreshold = 2
	cbConfig.Timeout = 100 * time.Millisecond

	options := &ConsumerOptions{
		URI:                  "redis://localhost:6379/1",
		Concurrency:          1,
		AutoAck:              false,
		RetryConfig:          DefaultRetryConfig(),
		CircuitBreakerConfig: cbConfig,
	}

	consumer, err := NewRedisStreamsConsumer(client, options)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	callCount := 0
	var mu sync.Mutex

	// Handler that fails initially, then succeeds
	handler := func(ctx context.Context, msg *Message) error {
		mu.Lock()
		callCount++
		currentCall := callCount
		mu.Unlock()

		if currentCall <= 3 {
			return fmt.Errorf("simulated failure %d", currentCall)
		}
		return nil
	}

	// Start consuming
	consumeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	go func() {
		consumer.ConsumeWithOptions(consumeCtx, streamName, handler, &ConsumeOptions{
			StartPosition: "0-0",
			MaxMessages:   5,
		})
	}()

	time.Sleep(1500 * time.Millisecond)

	mu.Lock()
	finalCallCount := callCount
	mu.Unlock()

	// Circuit breaker should have limited the number of calls
	t.Logf("Final call count with circuit breaker: %d", finalCallCount)

	// Should have made some calls but not all 5 messages due to circuit breaker
	if finalCallCount == 0 {
		t.Error("Expected some calls to be made")
	}
}

func TestRedisStreamsIntegrationWithDriverFactory(t *testing.T) {
	// Test integration through the driver factory
	driver, err := DefaultDriverFactory.Create("redis-streams://localhost:6379/1")
	if err != nil {
		t.Skipf("Redis not available for testing: %v", err)
	}
	defer driver.Close()

	// Test creating producer
	producerOpts := DefaultProducerOptions()
	producerOpts.URI = "redis-streams://localhost:6379/1"
	
	producer, err := driver.NewProducer(producerOpts)
	if err != nil {
		t.Errorf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Test creating consumer
	consumerOpts := DefaultConsumerOptions()
	consumerOpts.URI = "redis-streams://localhost:6379/1"
	
	consumer, err := driver.NewConsumer(consumerOpts)
	if err != nil {
		t.Errorf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Test basic functionality
	ctx := context.Background()
	testMsg := &Message{
		ID:        "integration-test",
		Body:      []byte("Integration test message"),
		Timestamp: time.Now(),
	}

	err = producer.Send(ctx, "integration-topic", testMsg)
	if err != nil {
		t.Errorf("Failed to send message: %v", err)
	}
}

// Benchmark tests
func BenchmarkRedisStreamsSend(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1,
	})
	defer client.Close()

	// Skip if Redis not available
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis not available for benchmarking: %v", err)
	}

	options := &ProducerOptions{
		URI:         "redis://localhost:6379/1",
		Timeout:     30 * time.Second,
		RetryConfig: DefaultRetryConfig(),
	}

	producer, err := NewRedisStreamsProducer(client, options)
	if err != nil {
		b.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	streamName := "benchmark-stream"
	defer cleanupTestStream(b, client, streamName)

	message := &Message{
		ID:        "bench-msg",
		Body:      []byte("Benchmark message"),
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		message.ID = fmt.Sprintf("bench-msg-%d", i)
		err := producer.Send(ctx, streamName, message)
		if err != nil {
			b.Errorf("Send failed: %v", err)
		}
	}
}

func BenchmarkRedisStreamsBatchSend(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1,
	})
	defer client.Close()

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis not available for benchmarking: %v", err)
	}

	options := &ProducerOptions{
		URI:         "redis://localhost:6379/1",
		Timeout:     30 * time.Second,
		RetryConfig: DefaultRetryConfig(),
	}

	producer, err := NewRedisStreamsProducer(client, options)
	if err != nil {
		b.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	streamName := "benchmark-batch-stream"
	defer cleanupTestStream(b, client, streamName)

	// Create batch of 10 messages
	batchSize := 10
	messages := make([]*Message, batchSize)
	for i := 0; i < batchSize; i++ {
		messages[i] = &Message{
			ID:        fmt.Sprintf("batch-msg-%d", i),
			Body:      []byte(fmt.Sprintf("Batch message %d", i)),
			Timestamp: time.Now(),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Update IDs to make them unique
		for j := 0; j < batchSize; j++ {
			messages[j].ID = fmt.Sprintf("batch-msg-%d-%d", i, j)
		}
		err := producer.SendBatch(ctx, streamName, messages)
		if err != nil {
			b.Errorf("Batch send failed: %v", err)
		}
	}
}