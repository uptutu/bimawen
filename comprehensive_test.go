package bimawen

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestSerializer implements Serializer for testing
type TestSerializer struct{}

func (s *TestSerializer) Serialize(data interface{}) ([]byte, error) {
	return []byte(fmt.Sprintf("test:%v", data)), nil
}

func (s *TestSerializer) Deserialize(data []byte, v interface{}) error {
	str := string(data)
	if len(str) > 5 && str[:5] == "test:" {
		if strPtr, ok := v.(*string); ok {
			*strPtr = str[5:]
			return nil
		}
	}
	return fmt.Errorf("invalid test format")
}

func (s *TestSerializer) ContentType() string {
	return "application/test"
}

// TestRetryMechanism tests the retry mechanism thoroughly
func TestRetryMechanism(t *testing.T) {
	t.Run("Successful operation", func(t *testing.T) {
		retryer := NewRetryer(DefaultRetryConfig())
		ctx := context.Background()
		
		callCount := 0
		err := retryer.Retry(ctx, func(ctx context.Context) error {
			callCount++
			return nil
		})
		
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		
		if callCount != 1 {
			t.Errorf("Expected 1 call, got %d", callCount)
		}
	})
	
	t.Run("Retry on retryable error", func(t *testing.T) {
		config := &RetryConfig{
			MaxRetries:      2,
			InitialInterval: 10 * time.Millisecond,
			MaxInterval:     100 * time.Millisecond,
			Multiplier:      2.0,
		}
		retryer := NewRetryer(config)
		ctx := context.Background()
		
		callCount := 0
		err := retryer.Retry(ctx, func(ctx context.Context) error {
			callCount++
			if callCount < 3 {
				return NewRetryableError(fmt.Errorf("temporary error"))
			}
			return nil
		})
		
		if err != nil {
			t.Errorf("Expected no error after retries, got %v", err)
		}
		
		if callCount != 3 {
			t.Errorf("Expected 3 calls, got %d", callCount)
		}
	})
	
	t.Run("Stop on non-retryable error", func(t *testing.T) {
		retryer := NewRetryer(DefaultRetryConfig())
		ctx := context.Background()
		
		callCount := 0
		err := retryer.Retry(ctx, func(ctx context.Context) error {
			callCount++
			return NewNonRetryableError(fmt.Errorf("permanent error"))
		})
		
		if err == nil {
			t.Error("Expected error but got none")
		}
		
		if callCount != 1 {
			t.Errorf("Expected 1 call, got %d", callCount)
		}
	})
	
	t.Run("Context cancellation", func(t *testing.T) {
		retryer := NewRetryer(DefaultRetryConfig())
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		
		callCount := 0
		err := retryer.Retry(ctx, func(ctx context.Context) error {
			callCount++
			time.Sleep(100 * time.Millisecond) // Longer than context timeout
			return NewRetryableError(fmt.Errorf("slow operation"))
		})
		
		if err == nil {
			t.Error("Expected context error but got none")
		}
		
		if err != context.DeadlineExceeded {
			t.Errorf("Expected context.DeadlineExceeded, got %v", err)
		}
	})
}

// TestSerializerExtensibility tests serializer registration and usage
func TestSerializerExtensibility(t *testing.T) {
	registry := NewSerializerRegistry()
	testSerializer := &TestSerializer{}
	
	// Register custom serializer
	registry.Register("test", testSerializer)
	
	// Test retrieval
	retrieved, err := registry.Get("test")
	if err != nil {
		t.Errorf("Failed to get registered serializer: %v", err)
	}
	
	if retrieved.ContentType() != "application/test" {
		t.Errorf("Expected content type 'application/test', got %s", retrieved.ContentType())
	}
	
	// Test serialization/deserialization
	original := "hello world"
	serialized, err := retrieved.Serialize(original)
	if err != nil {
		t.Errorf("Serialization failed: %v", err)
	}
	
	var deserialized string
	err = retrieved.Deserialize(serialized, &deserialized)
	if err != nil {
		t.Errorf("Deserialization failed: %v", err)
	}
	
	if deserialized != original {
		t.Errorf("Expected %s, got %s", original, deserialized)
	}
}

// TestMessageValidation tests message validation
func TestMessageValidation(t *testing.T) {
	tests := []struct {
		name    string
		message *Message
		valid   bool
	}{
		{
			name: "Valid message",
			message: &Message{
				ID:        "test-123",
				Body:      []byte("test body"),
				Timestamp: time.Now(),
			},
			valid: true,
		},
		{
			name: "Empty body",
			message: &Message{
				ID:        "test-124",
				Body:      nil,
				Timestamp: time.Now(),
			},
			valid: false,
		},
		{
			name: "Empty ID",
			message: &Message{
				ID:        "",
				Body:      []byte("test body"),
				Timestamp: time.Now(),
			},
			valid: false,
		},
		{
			name: "Zero timestamp",
			message: &Message{
				ID:        "test-125",
				Body:      []byte("test body"),
				Timestamp: time.Time{},
			},
			valid: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := validateMessage(tt.message)
			if valid != tt.valid {
				t.Errorf("Expected valid=%v, got %v", tt.valid, valid)
			}
		})
	}
}

// TestConcurrency tests concurrent operations
func TestConcurrency(t *testing.T) {
	t.Run("Concurrent serializer access", func(t *testing.T) {
		serializer := NewJSONSerializer()
		
		var wg sync.WaitGroup
		errors := make(chan error, 10)
		
		// Run 10 concurrent serializations
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				
				data := map[string]interface{}{
					"id":      id,
					"message": fmt.Sprintf("test message %d", id),
				}
				
				serialized, err := serializer.Serialize(data)
				if err != nil {
					errors <- err
					return
				}
				
				var result map[string]interface{}
				err = serializer.Deserialize(serialized, &result)
				if err != nil {
					errors <- err
					return
				}
				
				if result["id"].(float64) != float64(id) {
					errors <- fmt.Errorf("expected id %d, got %v", id, result["id"])
				}
			}(i)
		}
		
		wg.Wait()
		close(errors)
		
		for err := range errors {
			if err != nil {
				t.Errorf("Concurrent operation error: %v", err)
			}
		}
	})
}

// TestErrorClassification tests error type classification
func TestErrorClassification(t *testing.T) {
	tests := []struct {
		name       string
		error      error
		retryable  bool
	}{
		{
			name:      "Retryable error",
			error:     NewRetryableError(fmt.Errorf("network timeout")),
			retryable: true,
		},
		{
			name:      "Non-retryable error",
			error:     NewNonRetryableError(fmt.Errorf("invalid data")),
			retryable: false,
		},
		{
			name:      "Context canceled",
			error:     context.Canceled,
			retryable: false,
		},
		{
			name:      "Context deadline exceeded",
			error:     context.DeadlineExceeded,
			retryable: false,
		},
		{
			name:      "Authentication error",
			error:     fmt.Errorf("authentication failed"),
			retryable: false,
		},
		{
			name:      "Validation error",
			error:     fmt.Errorf("invalid input data"),
			retryable: false,
		},
		{
			name:      "Generic error",
			error:     fmt.Errorf("some random error"),
			retryable: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapped := WrapRetryableError(tt.error)
			retryable := IsRetryableError(wrapped)
			
			if retryable != tt.retryable {
				t.Errorf("Expected retryable=%v, got %v", tt.retryable, retryable)
			}
		})
	}
}

// TestDefaultConfigurations tests default configuration values
func TestDefaultConfigurations(t *testing.T) {
	t.Run("Default retry config", func(t *testing.T) {
		config := DefaultRetryConfig()
		
		if config.MaxRetries != 3 {
			t.Errorf("Expected MaxRetries=3, got %d", config.MaxRetries)
		}
		
		if config.InitialInterval != 1*time.Second {
			t.Errorf("Expected InitialInterval=1s, got %v", config.InitialInterval)
		}
		
		if config.Multiplier != 2.0 {
			t.Errorf("Expected Multiplier=2.0, got %f", config.Multiplier)
		}
	})
	
	t.Run("Default producer options", func(t *testing.T) {
		opts := DefaultProducerOptions()
		
		if opts.Timeout != 30*time.Second {
			t.Errorf("Expected Timeout=30s, got %v", opts.Timeout)
		}
		
		if opts.BatchSize != 100 {
			t.Errorf("Expected BatchSize=100, got %d", opts.BatchSize)
		}
		
		if opts.EnableConfirms {
			t.Error("Default options should not have confirms enabled")
		}
	})
	
	t.Run("Default consumer options", func(t *testing.T) {
		opts := DefaultConsumerOptions()
		
		if opts.Concurrency != 10 {
			t.Errorf("Expected Concurrency=10, got %d", opts.Concurrency)
		}
		
		if opts.AutoAck {
			t.Error("Default options should not have AutoAck enabled")
		}
	})
}

// validateMessage validates a message structure
func validateMessage(msg *Message) bool {
	if msg == nil {
		return false
	}
	
	if msg.ID == "" {
		return false
	}
	
	if msg.Body == nil || len(msg.Body) == 0 {
		return false
	}
	
	if msg.Timestamp.IsZero() {
		return false
	}
	
	return true
}

// TestURIParsingEdgeCases tests edge cases in URI parsing
func TestURIParsingEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		expectError bool
		expected    *URIInfo
	}{
		{
			name:        "Malformed URI",
			uri:         "://invalid",
			expectError: true,
		},
		{
			name:        "Missing host",
			uri:         "amqp://:5672/",
			expectError: false,
			expected: &URIInfo{
				DriverType: DriverRabbitMQ,
				Host:       "localhost",
				Port:       5672,
			},
		},
		{
			name:        "Invalid port",
			uri:         "amqp://localhost:invalid/",
			expectError: true,
		},
		{
			name:        "URI with special characters",
			uri:         "amqp://user%40domain:pass%23word@host:5672/vhost%2Ftest",
			expectError: false,
			expected: &URIInfo{
				DriverType: DriverRabbitMQ,
				Host:       "host",
				Port:       5672,
				Username:   "user@domain",
				Password:   "pass#word",
				VHost:      "/vhost/test",
			},
		},
		{
			name:        "Redis with negative database",
			uri:         "redis://localhost:6379/-1",
			expectError: true,
		},
		{
			name:        "Multiple query parameters",
			uri:         "amqp://localhost:5672/?tls=true&max_connections=10&timeout=30s",
			expectError: false,
			expected: &URIInfo{
				DriverType: DriverRabbitMQ,
				Host:       "localhost",
				Port:       5672,
				Options: map[string]string{
					"tls":             "true",
					"max_connections": "10",
					"timeout":         "30s",
				},
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseURI(tt.uri)
			
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			if tt.expected != nil {
				if result.DriverType != tt.expected.DriverType {
					t.Errorf("DriverType: expected %v, got %v", tt.expected.DriverType, result.DriverType)
				}
				
				if result.Host != tt.expected.Host {
					t.Errorf("Host: expected %s, got %s", tt.expected.Host, result.Host)
				}
				
				if result.Port != tt.expected.Port {
					t.Errorf("Port: expected %d, got %d", tt.expected.Port, result.Port)
				}
				
				if tt.expected.Options != nil {
					for k, v := range tt.expected.Options {
						if result.Options[k] != v {
							t.Errorf("Option %s: expected %s, got %s", k, v, result.Options[k])
						}
					}
				}
			}
		})
	}
}