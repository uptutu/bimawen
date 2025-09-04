package bimawen

import (
	"context"
	"testing"
	"time"
)

func TestConnectionPool(t *testing.T) {
	// Skip if no RabbitMQ available
	if testing.Short() {
		t.Skip("skipping connection pool test in short mode")
	}

	config := DefaultConnectionPoolConfig()
	config.MaxConnections = 2
	config.MaxChannels = 10

	// Use a mock URI for testing
	pool, err := NewConnectionPool("amqp://guest:guest@localhost:5672/", config)
	if err != nil {
		t.Skipf("Skipping test - RabbitMQ not available: %v", err)
	}
	defer pool.Close()

	// Test health check
	ctx := context.Background()
	if err := pool.HealthCheck(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}

	// Test getting channels
	ch1, err := pool.GetChannel()
	if err != nil {
		t.Errorf("Failed to get channel: %v", err)
	}
	
	ch2, err := pool.GetChannel()
	if err != nil {
		t.Errorf("Failed to get second channel: %v", err)
	}

	// Return channels
	pool.ReturnChannel(ch1)
	pool.ReturnChannel(ch2)

	// Test stats
	stats := pool.Stats()
	if stats["max_connections"] != 2 {
		t.Errorf("Expected max_connections=2, got %v", stats["max_connections"])
	}
	if stats["max_channels"] != 10 {
		t.Errorf("Expected max_channels=10, got %v", stats["max_channels"])
	}
}

func TestRabbitMQDriverWithPool(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping driver pool test in short mode")
	}

	// Test URI with pool options
	info := &URIInfo{
		DriverType: DriverRabbitMQ,
		Host:       "localhost",
		Port:       5672,
		Username:   "guest",
		Password:   "guest",
		VHost:      "/",
		Options: map[string]string{
			"max_connections": "3",
			"max_channels":    "15",
		},
	}

	driver, err := NewRabbitMQDriver(info)
	if err != nil {
		t.Skipf("Skipping test - RabbitMQ not available: %v", err)
	}
	defer driver.Close()

	// Test health check
	ctx := context.Background()
	if err := driver.HealthCheck(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}

	// Create producer and consumer
	producerOpts := DefaultProducerOptions()
	producer, err := driver.NewProducer(producerOpts)
	if err != nil {
		t.Errorf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	consumerOpts := DefaultConsumerOptions()
	consumer, err := driver.NewConsumer(consumerOpts)
	if err != nil {
		t.Errorf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Test sending a message
	message := &Message{
		ID:        "test-pool-msg",
		Body:      []byte("test message with pool"),
		Headers:   map[string]interface{}{"test": "pool"},
		Timestamp: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = producer.Send(ctx, "test-pool-queue", message)
	if err != nil {
		t.Errorf("Failed to send message: %v", err)
	}
}

func TestURIOptionsForPool(t *testing.T) {
	tests := []struct {
		name     string
		uri      string
		expected map[string]string
	}{
		{
			name: "URI with pool options",
			uri:  "amqp://guest:guest@localhost:5672/?max_connections=5&max_channels=50",
			expected: map[string]string{
				"max_connections": "5",
				"max_channels":    "50",
			},
		},
		{
			name:     "URI without options",
			uri:      "amqp://guest:guest@localhost:5672/",
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := ParseURI(tt.uri)
			if err != nil {
				t.Errorf("Failed to parse URI: %v", err)
				return
			}

			for key, expectedValue := range tt.expected {
				if actualValue, ok := info.Options[key]; !ok || actualValue != expectedValue {
					t.Errorf("Option %s: expected %s, got %s", key, expectedValue, actualValue)
				}
			}
		})
	}
}