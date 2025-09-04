package bimawen

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestDeadLetterConfig(t *testing.T) {
	t.Run("Default config", func(t *testing.T) {
		config := DefaultDeadLetterConfig("test-queue")
		
		if config.Enabled {
			t.Error("Default config should be disabled")
		}
		
		if config.QueueName != "test-queue.dlq" {
			t.Errorf("Expected QueueName 'test-queue.dlq', got %s", config.QueueName)
		}
		
		if config.ExchangeName != "test-queue.dlx" {
			t.Errorf("Expected ExchangeName 'test-queue.dlx', got %s", config.ExchangeName)
		}
		
		if config.MaxRetries != 3 {
			t.Errorf("Expected MaxRetries 3, got %d", config.MaxRetries)
		}
	})
	
	t.Run("Enabled config", func(t *testing.T) {
		config := DefaultDeadLetterConfig("orders")
		config.Enabled = true
		config.MaxRetries = 5
		config.RetryDelay = 10 * time.Second
		
		if !config.Enabled {
			t.Error("Config should be enabled")
		}
		
		if config.MaxRetries != 5 {
			t.Errorf("Expected MaxRetries 5, got %d", config.MaxRetries)
		}
		
		if config.RetryDelay != 10*time.Second {
			t.Errorf("Expected RetryDelay 10s, got %v", config.RetryDelay)
		}
	})
}

func TestDeadLetterMessage(t *testing.T) {
	originalMsg := &Message{
		ID:        "test-123",
		Body:      []byte("test message"),
		Headers:   map[string]interface{}{"source": "test"},
		Timestamp: time.Now(),
	}
	
	dlMsg := &DeadLetterMessage{
		OriginalMessage: originalMsg,
		Reason:          "processing failed",
		FailureCount:    2,
		LastFailure:     time.Now(),
		FirstFailure:    time.Now().Add(-1 * time.Hour),
		OriginalQueue:   "test-queue",
		StackTrace:      "test stack trace",
	}
	
	if dlMsg.OriginalMessage.ID != "test-123" {
		t.Errorf("Expected original message ID 'test-123', got %s", dlMsg.OriginalMessage.ID)
	}
	
	if dlMsg.Reason != "processing failed" {
		t.Errorf("Expected reason 'processing failed', got %s", dlMsg.Reason)
	}
	
	if dlMsg.FailureCount != 2 {
		t.Errorf("Expected failure count 2, got %d", dlMsg.FailureCount)
	}
}

func TestDeadLetterHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dead letter handler test in short mode")
	}
	
	// This test would require a real RabbitMQ connection
	// For now, we test the configuration and setup logic
	
	config := DefaultDeadLetterConfig("test-dlq")
	config.Enabled = false
	
	// Test disabled handler
	_, err := NewDeadLetterHandler(nil, config)
	if err == nil {
		t.Error("Expected error for disabled dead letter queue")
	}
}

func TestConsumerWithDeadLetterQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping consumer DLQ test in short mode")
	}
	
	// Test consumer options with dead letter config
	dlConfig := DefaultDeadLetterConfig("test-consumer")
	dlConfig.Enabled = true
	dlConfig.MaxRetries = 2
	dlConfig.RetryDelay = 1 * time.Second
	
	consumerOpts := DefaultConsumerOptions()
	consumerOpts.DeadLetterConfig = dlConfig
	
	if consumerOpts.DeadLetterConfig == nil {
		t.Error("Dead letter config should not be nil")
	}
	
	if !consumerOpts.DeadLetterConfig.Enabled {
		t.Error("Dead letter config should be enabled")
	}
	
	if consumerOpts.DeadLetterConfig.MaxRetries != 2 {
		t.Errorf("Expected MaxRetries 2, got %d", consumerOpts.DeadLetterConfig.MaxRetries)
	}
}

func TestDeadLetterStats(t *testing.T) {
	stats := &DeadLetterStats{
		QueueName:     "test.dlq",
		MessageCount:  5,
		ConsumerCount: 0,
	}
	
	if stats.QueueName != "test.dlq" {
		t.Errorf("Expected queue name 'test.dlq', got %s", stats.QueueName)
	}
	
	if stats.MessageCount != 5 {
		t.Errorf("Expected message count 5, got %d", stats.MessageCount)
	}
	
	if stats.ConsumerCount != 0 {
		t.Errorf("Expected consumer count 0, got %d", stats.ConsumerCount)
	}
}

// Mock message handler for testing
func mockSuccessHandler(ctx context.Context, msg *Message) error {
	return nil
}

func mockFailHandler(ctx context.Context, msg *Message) error {
	return fmt.Errorf("processing failed")
}

func mockRetryableFailHandler(ctx context.Context, msg *Message) error {
	return NewRetryableError(fmt.Errorf("retryable failure"))
}

func mockNonRetryableFailHandler(ctx context.Context, msg *Message) error {
	return NewNonRetryableError(fmt.Errorf("non-retryable failure"))
}

func TestDeadLetterMessageHandling(t *testing.T) {
	// Test different error scenarios without actual RabbitMQ
	
	t.Run("Successful processing", func(t *testing.T) {
		err := mockSuccessHandler(context.Background(), &Message{ID: "test"})
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})
	
	t.Run("Failed processing", func(t *testing.T) {
		err := mockFailHandler(context.Background(), &Message{ID: "test"})
		if err == nil {
			t.Error("Expected error but got none")
		}
	})
	
	t.Run("Retryable failure", func(t *testing.T) {
		err := mockRetryableFailHandler(context.Background(), &Message{ID: "test"})
		if err == nil {
			t.Error("Expected error but got none")
		}
		
		if !IsRetryableError(err) {
			t.Error("Error should be retryable")
		}
	})
	
	t.Run("Non-retryable failure", func(t *testing.T) {
		err := mockNonRetryableFailHandler(context.Background(), &Message{ID: "test"})
		if err == nil {
			t.Error("Expected error but got none")
		}
		
		if IsRetryableError(err) {
			t.Error("Error should not be retryable")
		}
	})
}

func TestDeadLetterConfigValidation(t *testing.T) {
	tests := []struct {
		name     string
		config   *DeadLetterConfig
		valid    bool
	}{
		{
			name: "Valid config",
			config: &DeadLetterConfig{
				Enabled:      true,
				QueueName:    "test.dlq",
				ExchangeName: "test.dlx",
				RoutingKey:   "dead",
				MaxRetries:   3,
			},
			valid: true,
		},
		{
			name: "Disabled config",
			config: &DeadLetterConfig{
				Enabled: false,
			},
			valid: true, // Disabled configs are always valid
		},
		{
			name: "Missing queue name",
			config: &DeadLetterConfig{
				Enabled:      true,
				QueueName:    "",
				ExchangeName: "test.dlx",
				RoutingKey:   "dead",
				MaxRetries:   3,
			},
			valid: false,
		},
		{
			name: "Missing exchange name",
			config: &DeadLetterConfig{
				Enabled:      true,
				QueueName:    "test.dlq",
				ExchangeName: "",
				RoutingKey:   "dead",
				MaxRetries:   3,
			},
			valid: false,
		},
		{
			name: "Zero max retries",
			config: &DeadLetterConfig{
				Enabled:      true,
				QueueName:    "test.dlq",
				ExchangeName: "test.dlx",
				RoutingKey:   "dead",
				MaxRetries:   0,
			},
			valid: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := validateDeadLetterConfig(tt.config)
			if valid != tt.valid {
				t.Errorf("Expected valid=%v, got %v", tt.valid, valid)
			}
		})
	}
}

// validateDeadLetterConfig validates a dead letter configuration
func validateDeadLetterConfig(config *DeadLetterConfig) bool {
	if config == nil {
		return false
	}
	
	if !config.Enabled {
		return true // Disabled configs don't need validation
	}
	
	if config.QueueName == "" {
		return false
	}
	
	if config.ExchangeName == "" {
		return false
	}
	
	if config.MaxRetries <= 0 {
		return false
	}
	
	return true
}

func TestDeadLetterRetryStrategies(t *testing.T) {
	tests := []struct {
		name     string
		config   *DeadLetterConfig
		attempts []int
		expected []time.Duration
	}{
		{
			name: "Fixed strategy",
			config: &DeadLetterConfig{
				RetryStrategy: RetryStrategyFixed,
				RetryDelay:   1 * time.Second,
			},
			attempts: []int{1, 2, 3, 4},
			expected: []time.Duration{1 * time.Second, 1 * time.Second, 1 * time.Second, 1 * time.Second},
		},
		{
			name: "Linear strategy",
			config: &DeadLetterConfig{
				RetryStrategy: RetryStrategyLinear,
				RetryDelay:   1 * time.Second,
			},
			attempts: []int{1, 2, 3, 4},
			expected: []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second},
		},
		{
			name: "Exponential strategy",
			config: &DeadLetterConfig{
				RetryStrategy:   RetryStrategyExponential,
				RetryDelay:     1 * time.Second,
				RetryMultiplier: 2.0,
			},
			attempts: []int{1, 2, 3, 4},
			expected: []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second},
		},
		{
			name: "Exponential with max delay",
			config: &DeadLetterConfig{
				RetryStrategy:   RetryStrategyExponential,
				RetryDelay:     1 * time.Second,
				RetryMultiplier: 2.0,
				MaxRetryDelay:  5 * time.Second,
			},
			attempts: []int{1, 2, 3, 4},
			expected: []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, attempt := range tt.attempts {
				delay := tt.config.calculateRetryDelay(attempt)
				expected := tt.expected[i]
				if delay != expected {
					t.Errorf("Attempt %d: expected delay %v, got %v", attempt, expected, delay)
				}
			}
		})
	}
}

func TestDeadLetterConfigWithCustomOptions(t *testing.T) {
	config := DefaultDeadLetterConfig("test-queue")
	config.Enabled = true
	config.RetryStrategy = RetryStrategyExponential
	config.RetryMultiplier = 3.0
	config.MaxRetryDelay = 5 * time.Minute
	config.AutoRequeue = true
	config.AutoRequeueDelay = 2 * time.Hour
	config.AlertThreshold = 500
	config.PreserveOriginalHeaders = false
	config.CustomHeaders = map[string]interface{}{
		"dlq-source": "test-service",
		"dlq-version": "1.0",
	}
	
	if config.RetryStrategy != RetryStrategyExponential {
		t.Errorf("Expected retry strategy %s, got %s", RetryStrategyExponential, config.RetryStrategy)
	}
	
	if config.RetryMultiplier != 3.0 {
		t.Errorf("Expected retry multiplier 3.0, got %v", config.RetryMultiplier)
	}
	
	if config.MaxRetryDelay != 5*time.Minute {
		t.Errorf("Expected max retry delay %v, got %v", 5*time.Minute, config.MaxRetryDelay)
	}
	
	if !config.AutoRequeue {
		t.Error("Expected AutoRequeue to be true")
	}
	
	if config.AutoRequeueDelay != 2*time.Hour {
		t.Errorf("Expected auto requeue delay %v, got %v", 2*time.Hour, config.AutoRequeueDelay)
	}
	
	if config.AlertThreshold != 500 {
		t.Errorf("Expected alert threshold 500, got %d", config.AlertThreshold)
	}
	
	if config.PreserveOriginalHeaders {
		t.Error("Expected PreserveOriginalHeaders to be false")
	}
	
	if len(config.CustomHeaders) != 2 {
		t.Errorf("Expected 2 custom headers, got %d", len(config.CustomHeaders))
	}
	
	if config.CustomHeaders["dlq-source"] != "test-service" {
		t.Errorf("Expected custom header dlq-source='test-service', got %v", config.CustomHeaders["dlq-source"])
	}
}

func TestDeadLetterConfigValidationEnhanced(t *testing.T) {
	tests := []struct {
		name     string
		config   *DeadLetterConfig
		valid    bool
	}{
		{
			name: "Valid enhanced config",
			config: &DeadLetterConfig{
				Enabled:      true,
				QueueName:    "test.dlq",
				ExchangeName: "test.dlx",
				RoutingKey:   "dead",
				MaxRetries:   3,
				RetryStrategy: RetryStrategyFixed,
				RetryMultiplier: 2.0,
				RetryDelay:   30 * time.Second,
				MaxRetryDelay: 10 * time.Minute,
			},
			valid: true,
		},
		{
			name: "Invalid retry strategy",
			config: &DeadLetterConfig{
				Enabled:      true,
				QueueName:    "test.dlq",
				ExchangeName: "test.dlx",
				RetryStrategy: "invalid-strategy",
			},
			valid: false,
		},
		{
			name: "Invalid retry multiplier",
			config: &DeadLetterConfig{
				Enabled:      true,
				QueueName:    "test.dlq",
				ExchangeName: "test.dlx",
				RetryMultiplier: -1.0,
			},
			valid: false,
		},
		{
			name: "Max retry delay less than base delay",
			config: &DeadLetterConfig{
				Enabled:      true,
				QueueName:    "test.dlq",
				ExchangeName: "test.dlx",
				RetryDelay:   1 * time.Minute,
				MaxRetryDelay: 30 * time.Second,
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.ValidateConfig()
			if tt.valid && err != nil {
				t.Errorf("Expected config to be valid, got error: %v", err)
			}
			if !tt.valid && err == nil {
				t.Error("Expected config to be invalid, got no error")
			}
		})
	}
}