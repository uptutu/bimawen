package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"local.git/libs/bimawen.git"
)

func main() {
	fmt.Println("=== Connection Accessor Example ===")

	// Example 1: RabbitMQ Connection Access
	fmt.Println("\n1. RabbitMQ Connection Access:")
	testRabbitMQConnectionAccess()

	// Example 2: Redis Connection Access
	fmt.Println("\n2. Redis Connection Access:")
	testRedisConnectionAccess()

	// Example 3: Redis Streams Connection Access
	fmt.Println("\n3. Redis Streams Connection Access:")
	testRedisStreamsConnectionAccess()

	fmt.Println("\n=== Connection Accessor Example Complete ===")
}

func testRabbitMQConnectionAccess() {
	// Try to connect to RabbitMQ
	producer, err := bimawen.NewProducer("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Printf("RabbitMQ not available: %v (this is normal if RabbitMQ is not running)\n", err)
		return
	}
	defer producer.Close()

	// Check if producer implements RabbitMQConnectionAccessor
	if accessor, ok := producer.(bimawen.RabbitMQConnectionAccessor); ok {
		fmt.Println("✅ Producer implements RabbitMQConnectionAccessor interface")

		// Get a channel for direct AMQP operations
		channel := accessor.GetChannel()
		if channel != nil {
			fmt.Println("✅ Successfully obtained AMQP channel")
			
			// Example: Use the channel directly for advanced operations
			err := channel.ExchangeDeclare(
				"direct-access-exchange", // name
				"direct",                 // type
				false,                    // durable
				false,                    // auto-delete
				false,                    // internal
				false,                    // no-wait
				nil,                      // arguments
			)
			if err != nil {
				log.Printf("Exchange declaration failed: %v", err)
			} else {
				fmt.Println("✅ Successfully declared exchange using direct channel access")
				
				// Clean up
				channel.ExchangeDelete("direct-access-exchange", false, false)
			}
			
			// Return channel to pool (important!)
			if rabbitProducer, ok := producer.(*bimawen.RabbitMQProducer); ok {
				rabbitProducer.GetChannel() // This will return the channel automatically
			}
		}

		// Get a connection for direct AMQP connection operations
		connection := accessor.GetConnection()
		if connection != nil && !connection.IsClosed() {
			fmt.Println("✅ Successfully obtained AMQP connection")
			fmt.Printf("   Connection state: Open (Server properties available)\n")
		}

	} else {
		fmt.Println("❌ Producer does not implement RabbitMQConnectionAccessor interface")
	}

	// Test consumer as well
	consumer, err := bimawen.NewConsumer("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Printf("RabbitMQ consumer not available: %v\n", err)
		return
	}
	defer consumer.Close()

	if accessor, ok := consumer.(bimawen.RabbitMQConnectionAccessor); ok {
		fmt.Println("✅ Consumer also implements RabbitMQConnectionAccessor interface")
		
		channel := accessor.GetChannel()
		if channel != nil {
			fmt.Println("✅ Successfully obtained consumer AMQP channel")
		}
	}
}

func testRedisConnectionAccess() {
	// Try to connect to Redis
	producer, err := bimawen.NewProducer("redis://localhost:6379/0")
	if err != nil {
		fmt.Printf("Redis not available: %v (this is normal if Redis is not running)\n", err)
		return
	}
	defer producer.Close()

	// Check if producer implements RedisConnectionAccessor
	if accessor, ok := producer.(bimawen.RedisConnectionAccessor); ok {
		fmt.Println("✅ Producer implements RedisConnectionAccessor interface")

		// Get Redis client for direct operations
		client := accessor.GetClient()
		if client != nil {
			fmt.Println("✅ Successfully obtained Redis client")

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// Example: Use the client directly for Redis operations
			pong := client.Ping(ctx)
			if pong.Err() == nil {
				fmt.Printf("✅ Redis PING successful: %s\n", pong.Val())

				// Example: Direct Redis operations
				err := client.Set(ctx, "direct-access-key", "direct-access-value", time.Minute).Err()
				if err == nil {
					fmt.Println("✅ Successfully set key using direct client access")

					// Get the value back
					val := client.Get(ctx, "direct-access-key")
					if val.Err() == nil {
						fmt.Printf("✅ Retrieved value: %s\n", val.Val())
					}

					// Clean up
					client.Del(ctx, "direct-access-key")
				}
			} else {
				fmt.Printf("Redis PING failed: %v\n", pong.Err())
			}
		}
	} else {
		fmt.Println("❌ Producer does not implement RedisConnectionAccessor interface")
	}

	// Test consumer as well
	consumer, err := bimawen.NewConsumer("redis://localhost:6379/0")
	if err != nil {
		fmt.Printf("Redis consumer not available: %v\n", err)
		return
	}
	defer consumer.Close()

	if accessor, ok := consumer.(bimawen.RedisConnectionAccessor); ok {
		fmt.Println("✅ Consumer also implements RedisConnectionAccessor interface")
		// We have the accessor, but don't need to do anything with it in this example
		_ = accessor
	}
}

func testRedisStreamsConnectionAccess() {
	// Try to connect to Redis Streams
	producer, err := bimawen.NewProducer("redis-streams://localhost:6379/0")
	if err != nil {
		fmt.Printf("Redis Streams not available: %v (this is normal if Redis is not running)\n", err)
		return
	}
	defer producer.Close()

	// Check if producer implements RedisStreamsConnectionAccessor
	if accessor, ok := producer.(bimawen.RedisStreamsConnectionAccessor); ok {
		fmt.Println("✅ Producer implements RedisStreamsConnectionAccessor interface")

		// Get Redis client for direct stream operations
		client := accessor.GetClient()
		if client != nil {
			fmt.Println("✅ Successfully obtained Redis Streams client")

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// Example: Use the client directly for Redis Streams operations
			streamName := "direct-access-stream"
			
			// Add an entry to the stream directly
			result := client.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     "*",
				Values: map[string]interface{}{
					"field1": "value1",
					"field2": "value2",
					"timestamp": time.Now().Unix(),
				},
			})
			
			if result.Err() == nil {
				fmt.Printf("✅ Successfully added entry to stream: %s\n", result.Val())

				// Read from the stream directly
				readResult := client.XRead(ctx, &redis.XReadArgs{
					Streams: []string{streamName, "0"},
					Count:   1,
					Block:   100 * time.Millisecond,
				})

				if readResult.Err() == nil && len(readResult.Val()) > 0 {
					fmt.Printf("✅ Successfully read from stream: %d entries\n", len(readResult.Val()[0].Messages))
				}

				// Clean up
				client.Del(ctx, streamName)
			} else {
				fmt.Printf("Failed to add to stream: %v\n", result.Err())
			}
		}
	} else {
		fmt.Println("❌ Producer does not implement RedisStreamsConnectionAccessor interface")
	}

	// Test consumer as well
	consumer, err := bimawen.NewConsumer("redis-streams://localhost:6379/0")
	if err != nil {
		fmt.Printf("Redis Streams consumer not available: %v\n", err)
		return
	}
	defer consumer.Close()

	if accessor, ok := consumer.(bimawen.RedisStreamsConnectionAccessor); ok {
		fmt.Println("✅ Consumer also implements RedisStreamsConnectionAccessor interface")
		// We have the accessor, but don't need to do anything with it in this example
		_ = accessor
	}
}