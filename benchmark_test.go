package bimawen

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkJSONSerialization benchmarks JSON serialization performance
func BenchmarkJSONSerialization(b *testing.B) {
	serializer := NewJSONSerializer()
	
	data := map[string]interface{}{
		"id":        "benchmark-test-123",
		"message":   "This is a benchmark test message",
		"timestamp": time.Now().Unix(),
		"data": map[string]interface{}{
			"nested":  true,
			"count":   42,
			"values":  []int{1, 2, 3, 4, 5},
		},
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		serialized, err := serializer.Serialize(data)
		if err != nil {
			b.Fatalf("Serialization failed: %v", err)
		}
		
		var result map[string]interface{}
		err = serializer.Deserialize(serialized, &result)
		if err != nil {
			b.Fatalf("Deserialization failed: %v", err)
		}
	}
}

// BenchmarkRetryMechanism benchmarks retry performance
func BenchmarkRetryMechanism(b *testing.B) {
	config := &RetryConfig{
		MaxRetries:      1,
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     10 * time.Millisecond,
		Multiplier:      2.0,
	}
	retryer := NewRetryer(config)
	ctx := context.Background()
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		err := retryer.Retry(ctx, func(ctx context.Context) error {
			return nil // Always succeed
		})
		if err != nil {
			b.Fatalf("Retry failed: %v", err)
		}
	}
}

// BenchmarkURIParsing benchmarks URI parsing performance
func BenchmarkURIParsing(b *testing.B) {
	uris := []string{
		"amqp://guest:guest@localhost:5672/",
		"amqps://user:pass@rabbitmq.example.com:5671/vhost?tls=true&max_connections=10",
		"redis://localhost:6379/0",
		"rediss://user:pass@redis.example.com:6380/1?timeout=30s&pool_size=20",
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		uri := uris[i%len(uris)]
		_, err := ParseURI(uri)
		if err != nil {
			b.Fatalf("URI parsing failed for %s: %v", uri, err)
		}
	}
}

// BenchmarkMessageValidation benchmarks message validation
func BenchmarkMessageValidation(b *testing.B) {
	message := &Message{
		ID:        "benchmark-msg-123",
		Body:      []byte("benchmark message body content"),
		Headers:   map[string]interface{}{"source": "benchmark"},
		Timestamp: time.Now(),
		Priority:  1,
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		valid := validateMessage(message)
		if !valid {
			b.Fatal("Message should be valid")
		}
	}
}

// BenchmarkConcurrentSerialization benchmarks concurrent serialization
func BenchmarkConcurrentSerialization(b *testing.B) {
	serializer := NewJSONSerializer()
	
	data := map[string]interface{}{
		"id":      "concurrent-test",
		"message": "concurrent serialization test",
		"count":   100,
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			serialized, err := serializer.Serialize(data)
			if err != nil {
				b.Fatalf("Serialization failed: %v", err)
			}
			
			var result map[string]interface{}
			err = serializer.Deserialize(serialized, &result)
			if err != nil {
				b.Fatalf("Deserialization failed: %v", err)
			}
		}
	})
}

// BenchmarkErrorWrapping benchmarks error wrapping performance
func BenchmarkErrorWrapping(b *testing.B) {
	baseError := fmt.Errorf("base error")
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		wrapped := WrapRetryableError(baseError)
		retryable := IsRetryableError(wrapped)
		if !retryable {
			b.Fatal("Error should be retryable")
		}
	}
}

// IntegrationBenchmarkExample shows how to write integration benchmarks
func IntegrationBenchmarkExample() {
	// This is an example of how you might benchmark the actual producer/consumer
	// It would only run if you have real RabbitMQ/Redis instances available
	fmt.Println("Integration benchmark example")
}