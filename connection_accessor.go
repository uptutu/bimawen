package bimawen

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

// ConnectionAccessor provides access to underlying driver connections
// This interface is generic and can be extended for different driver types

// RabbitMQConnectionAccessor provides access to RabbitMQ specific connections
type RabbitMQConnectionAccessor interface {
	// GetChannel returns the RabbitMQ AMQP channel
	GetChannel() *amqp.Channel
	
	// GetConnection returns the RabbitMQ AMQP connection
	GetConnection() *amqp.Connection
}

// RedisConnectionAccessor provides access to Redis specific connections
type RedisConnectionAccessor interface {
	// GetClient returns the Redis client
	GetClient() *redis.Client
}

// RedisStreamsConnectionAccessor provides access to Redis Streams specific connections
type RedisStreamsConnectionAccessor interface {
	// GetClient returns the Redis client for streams
	GetClient() *redis.Client
}