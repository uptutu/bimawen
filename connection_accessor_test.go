package bimawen

import (
	"context"
	"testing"
	"time"
	
	"github.com/redis/go-redis/v9"
)

func TestRabbitMQConnectionAccessor(t *testing.T) {
	// Skip if RabbitMQ is not available
	uri := "amqp://guest:guest@localhost:5672/"
	info, err := ParseURI(uri)
	if err != nil {
		t.Skipf("Skipping RabbitMQ connection accessor test: %v", err)
	}

	driver, err := NewRabbitMQDriver(info)
	if err != nil {
		t.Skipf("Skipping RabbitMQ connection accessor test - RabbitMQ not available: %v", err)
	}
	defer driver.Close()

	// Test health check first
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	if err := driver.HealthCheck(ctx); err != nil {
		t.Skipf("Skipping RabbitMQ connection accessor test - RabbitMQ not healthy: %v", err)
	}

	// Test Producer Connection Access
	producerOptions := DefaultProducerOptions()
	producer, err := driver.NewProducer(producerOptions)
	if err != nil {
		t.Fatalf("Failed to create RabbitMQ producer: %v", err)
	}
	defer producer.Close()

	// Test that producer implements RabbitMQConnectionAccessor
	if accessor, ok := producer.(RabbitMQConnectionAccessor); ok {
		// Test GetChannel
		channel := accessor.GetChannel()
		if channel == nil {
			t.Error("Expected non-nil channel from GetChannel()")
		} else {
			// Test that we can use the channel
			err := channel.ExchangeDeclare("test-exchange", "direct", false, false, false, false, nil)
			if err == nil {
				// Clean up
				channel.ExchangeDelete("test-exchange", false, false)
			}
			// Return channel to pool
			if rabbitProducer, ok := producer.(*RabbitMQProducer); ok {
				rabbitProducer.pool.ReturnChannel(channel)
			}
		}

		// Test GetConnection
		connection := accessor.GetConnection()
		if connection == nil {
			t.Error("Expected non-nil connection from GetConnection()")
		} else {
			// Test that we can use the connection
			if !connection.IsClosed() {
				t.Log("Connection is active and usable")
			}
		}
	} else {
		t.Error("RabbitMQ producer does not implement RabbitMQConnectionAccessor interface")
	}

	// Test Consumer Connection Access
	consumerOptions := DefaultConsumerOptions()
	consumerOptions.AutoAck = true
	consumer, err := driver.NewConsumer(consumerOptions)
	if err != nil {
		t.Fatalf("Failed to create RabbitMQ consumer: %v", err)
	}
	defer consumer.Close()

	// Test that consumer implements RabbitMQConnectionAccessor
	if accessor, ok := consumer.(RabbitMQConnectionAccessor); ok {
		// Test GetChannel
		channel := accessor.GetChannel()
		if channel == nil {
			t.Error("Expected non-nil channel from consumer GetChannel()")
		} else {
			// Test basic channel functionality
			_, err := channel.QueueDeclare("test-queue", false, true, false, false, nil)
			if err == nil {
				// Clean up
				channel.QueueDelete("test-queue", false, false, false)
			}
			// Return channel to pool
			if rabbitConsumer, ok := consumer.(*RabbitMQConsumer); ok {
				rabbitConsumer.pool.ReturnChannel(channel)
			}
		}

		// Test GetConnection
		connection := accessor.GetConnection()
		if connection == nil {
			t.Error("Expected non-nil connection from consumer GetConnection()")
		} else {
			if !connection.IsClosed() {
				t.Log("Consumer connection is active and usable")
			}
		}
	} else {
		t.Error("RabbitMQ consumer does not implement RabbitMQConnectionAccessor interface")
	}
}

func TestRedisConnectionAccessor(t *testing.T) {
	// Test Redis connection accessor
	uri := "redis://localhost:6379/0"
	info, err := ParseURI(uri)
	if err != nil {
		t.Skipf("Skipping Redis connection accessor test: %v", err)
	}

	driver, err := NewRedisDriver(info)
	if err != nil {
		t.Skipf("Skipping Redis connection accessor test - Redis not available: %v", err)
	}
	defer driver.Close()

	// Test health check first
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	if err := driver.HealthCheck(ctx); err != nil {
		t.Skipf("Skipping Redis connection accessor test - Redis not healthy: %v", err)
	}

	// Test Producer Connection Access
	producerOptions := DefaultProducerOptions()
	producer, err := driver.NewProducer(producerOptions)
	if err != nil {
		t.Fatalf("Failed to create Redis producer: %v", err)
	}
	defer producer.Close()

	// Test that producer implements RedisConnectionAccessor
	if accessor, ok := producer.(RedisConnectionAccessor); ok {
		client := accessor.GetClient()
		if client == nil {
			t.Error("Expected non-nil client from GetClient()")
		} else {
			// Test that we can use the client
			pong := client.Ping(ctx)
			if pong.Err() != nil {
				t.Errorf("Client ping failed: %v", pong.Err())
			} else {
				t.Log("Redis client is working correctly")
			}
		}
	} else {
		t.Error("Redis producer does not implement RedisConnectionAccessor interface")
	}

	// Test Consumer Connection Access
	consumerOptions := DefaultConsumerOptions()
	consumer, err := driver.NewConsumer(consumerOptions)
	if err != nil {
		t.Fatalf("Failed to create Redis consumer: %v", err)
	}
	defer consumer.Close()

	// Test that consumer implements RedisConnectionAccessor
	if accessor, ok := consumer.(RedisConnectionAccessor); ok {
		client := accessor.GetClient()
		if client == nil {
			t.Error("Expected non-nil client from consumer GetClient()")
		} else {
			// Test basic client functionality
			result := client.Set(ctx, "test-key", "test-value", time.Minute)
			if result.Err() != nil {
				t.Errorf("Client Set failed: %v", result.Err())
			} else {
				// Clean up
				client.Del(ctx, "test-key")
				t.Log("Redis consumer client is working correctly")
			}
		}
	} else {
		t.Error("Redis consumer does not implement RedisConnectionAccessor interface")
	}
}

func TestRedisStreamsConnectionAccessor(t *testing.T) {
	// Test Redis Streams connection accessor
	uri := "redis-streams://localhost:6379/0"
	info, err := ParseURI(uri)
	if err != nil {
		t.Skipf("Skipping Redis Streams connection accessor test: %v", err)
	}

	driver, err := NewRedisStreamsDriver(info)
	if err != nil {
		t.Skipf("Skipping Redis Streams connection accessor test - Redis not available: %v", err)
	}
	defer driver.Close()

	// Test health check first
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	if err := driver.HealthCheck(ctx); err != nil {
		t.Skipf("Skipping Redis Streams connection accessor test - Redis not healthy: %v", err)
	}

	// Test Producer Connection Access
	producerOptions := DefaultProducerOptions()
	producer, err := driver.NewProducer(producerOptions)
	if err != nil {
		t.Fatalf("Failed to create Redis Streams producer: %v", err)
	}
	defer producer.Close()

	// Test that producer implements RedisStreamsConnectionAccessor
	if accessor, ok := producer.(RedisStreamsConnectionAccessor); ok {
		client := accessor.GetClient()
		if client == nil {
			t.Error("Expected non-nil client from Redis Streams GetClient()")
		} else {
			// Test streams-specific functionality
			streamName := "test-stream"
			result := client.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     "*",
				Values: map[string]interface{}{
					"test": "data",
				},
			})
			if result.Err() != nil {
				t.Errorf("XAdd failed: %v", result.Err())
			} else {
				// Clean up
				client.Del(ctx, streamName)
				t.Log("Redis Streams producer client is working correctly")
			}
		}
	} else {
		t.Error("Redis Streams producer does not implement RedisStreamsConnectionAccessor interface")
	}

	// Test Consumer Connection Access
	consumerOptions := DefaultConsumerOptions()
	consumer, err := driver.NewConsumer(consumerOptions)
	if err != nil {
		t.Fatalf("Failed to create Redis Streams consumer: %v", err)
	}
	defer consumer.Close()

	// Test that consumer implements RedisStreamsConnectionAccessor
	if accessor, ok := consumer.(RedisStreamsConnectionAccessor); ok {
		client := accessor.GetClient()
		if client == nil {
			t.Error("Expected non-nil client from Redis Streams consumer GetClient()")
		} else {
			// Test basic client functionality
			pong := client.Ping(ctx)
			if pong.Err() != nil {
				t.Errorf("Consumer client ping failed: %v", pong.Err())
			} else {
				t.Log("Redis Streams consumer client is working correctly")
			}
		}
	} else {
		t.Error("Redis Streams consumer does not implement RedisStreamsConnectionAccessor interface")
	}
}

func TestConnectionAccessorWithClosedProducers(t *testing.T) {
	// Test that closed producers/consumers return nil from connection accessors
	
	// Test with Redis (simpler setup)
	uri := "redis://localhost:6379/0"
	info, err := ParseURI(uri)
	if err != nil {
		t.Skipf("Skipping closed connection test: %v", err)
	}

	driver, err := NewRedisDriver(info)
	if err != nil {
		t.Skipf("Skipping closed connection test - Redis not available: %v", err)
	}
	defer driver.Close()

	producer, err := driver.NewProducer(DefaultProducerOptions())
	if err != nil {
		t.Skipf("Failed to create producer: %v", err)
	}

	// Test before closing
	if accessor, ok := producer.(RedisConnectionAccessor); ok {
		client := accessor.GetClient()
		if client == nil {
			t.Error("Expected non-nil client before closing")
		}
	}

	// Close the producer
	producer.Close()

	// Test after closing
	if accessor, ok := producer.(RedisConnectionAccessor); ok {
		client := accessor.GetClient()
		if client != nil {
			t.Error("Expected nil client after closing producer")
		}
	}
}