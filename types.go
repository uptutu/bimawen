package bimawen

import (
	"context"
	"time"
)

// RabbitMQ driver option keys
const (
	// RabbitMQExchange specifies the exchange name for publishing messages (default: "")
	RabbitMQExchange = "exchange"
	// RabbitMQMandatory specifies if publishing should be mandatory (default: false)
	RabbitMQMandatory = "mandatory"
	// RabbitMQImmediate specifies if publishing should be immediate (default: false)
	RabbitMQImmediate = "immediate"
)

// Message represents a message in the message queue
type Message struct {
	// ID is the unique identifier of the message
	ID string
	
	// Topic is the topic/queue name where the message belongs
	Topic string
	
	// Body is the actual message content
	Body []byte
	
	// Headers contains message metadata
	Headers map[string]interface{}
	
	// Timestamp when the message was created
	Timestamp time.Time
	
	// DeliveryCount tracks how many times this message has been delivered
	DeliveryCount int
	
	// Priority of the message (0-255, higher means more priority)
	Priority uint8
	
	// TTL (Time To Live) for the message
	TTL time.Duration
	
	// Delay before the message becomes available for consumption
	Delay time.Duration
}

// Producer defines the interface for message producers
type Producer interface {
	// Send sends a message synchronously
	Send(ctx context.Context, topic string, message *Message) error
	
	// SendAsync sends a message asynchronously and returns a result channel
	SendAsync(ctx context.Context, topic string, message *Message) <-chan error
	
	// SendBatch sends multiple messages in a batch
	SendBatch(ctx context.Context, topic string, messages []*Message) error
	
	// Close closes the producer and releases resources
	Close() error
}

// Consumer defines the interface for message consumers
type Consumer interface {
	// Consume starts consuming messages from a topic
	Consume(ctx context.Context, topic string, handler MessageHandler) error
	
	// ConsumeWithOptions starts consuming with specific options
	ConsumeWithOptions(ctx context.Context, topic string, handler MessageHandler, opts *ConsumeOptions) error
	
	// Close closes the consumer and releases resources
	Close() error
}

// MessageHandler is the function signature for handling consumed messages
type MessageHandler func(ctx context.Context, message *Message) error

// Driver defines the interface that all MQ drivers must implement
type Driver interface {
	// NewProducer creates a new producer instance
	NewProducer(options *ProducerOptions) (Producer, error)
	
	// NewConsumer creates a new consumer instance
	NewConsumer(options *ConsumerOptions) (Consumer, error)
	
	// HealthCheck checks if the connection is healthy
	HealthCheck(ctx context.Context) error
	
	// Close closes the driver and all its connections
	Close() error
}

// Serializer defines the interface for message serialization
type Serializer interface {
	// Serialize converts data to bytes
	Serialize(data interface{}) ([]byte, error)
	
	// Deserialize converts bytes back to data
	Deserialize(data []byte, v interface{}) error
	
	// ContentType returns the content type string
	ContentType() string
}

// ProducerOptions contains configuration options for producers
type ProducerOptions struct {
	// URI connection string
	URI string
	
	// Serializer for message encoding (defaults to JSON)
	Serializer Serializer
	
	// Timeout for send operations
	Timeout time.Duration
	
	// RetryConfig for failed operations
	RetryConfig *RetryConfig
	
	// BatchSize for batch operations
	BatchSize int
	
	// EnableConfirms enables publisher confirms (RabbitMQ only)
	EnableConfirms bool
	
	// CircuitBreakerConfig for circuit breaker support
	CircuitBreakerConfig *CircuitBreakerConfig
	
	// Additional driver-specific options
	// For RabbitMQ:
	//   - "exchange": string - Exchange name for publishing (default: "")
	//   - "mandatory": bool - Mandatory publishing flag (default: false)
	//   - "immediate": bool - Immediate publishing flag (default: false)
	DriverOptions map[string]interface{}
}

// ConsumerOptions contains configuration options for consumers
type ConsumerOptions struct {
	// URI connection string
	URI string
	
	// Serializer for message decoding (defaults to JSON)
	Serializer Serializer
	
	// Concurrency level (number of concurrent message handlers)
	Concurrency int
	
	// AutoAck whether to automatically acknowledge messages
	AutoAck bool
	
	// RetryConfig for failed message processing
	RetryConfig *RetryConfig
	
	// DeadLetterConfig for dead letter queue support
	DeadLetterConfig *DeadLetterConfig
	
	// CircuitBreakerConfig for circuit breaker support
	CircuitBreakerConfig *CircuitBreakerConfig
	
	// Additional driver-specific options
	DriverOptions map[string]interface{}
}

// ConsumeOptions contains runtime options for consuming messages
type ConsumeOptions struct {
	// ConsumerGroup for grouped consumption (if supported by driver)
	ConsumerGroup string
	
	// ConsumerName unique name for this consumer instance (used with consumer groups)
	ConsumerName string
	
	// StartPosition for stream consumption (latest, earliest, or specific ID)
	StartPosition string
	
	// BlockTime how long to block when waiting for new messages
	BlockTime time.Duration
	
	// MaxMessages limits the number of messages to consume
	MaxMessages int
	
	// Timeout for consume operation
	Timeout time.Duration
}

// RetryConfig defines retry behavior configuration
type RetryConfig struct {
	// MaxRetries maximum number of retry attempts
	MaxRetries int
	
	// InitialInterval initial retry interval
	InitialInterval time.Duration
	
	// MaxInterval maximum retry interval
	MaxInterval time.Duration
	
	// Multiplier for exponential backoff
	Multiplier float64
	
	// RandomizationFactor adds jitter to retry intervals
	RandomizationFactor float64
}

// ConnectionConfig contains common connection configuration
type ConnectionConfig struct {
	// URI connection string
	URI string
	
	// ConnectTimeout timeout for establishing connection
	ConnectTimeout time.Duration
	
	// KeepAlive interval for connection health checks
	KeepAlive time.Duration
	
	// MaxRetries for connection attempts
	MaxRetries int
	
	// TLS configuration
	TLSConfig interface{}
}

// DefaultRetryConfig returns a sensible default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:          3,
		InitialInterval:     1 * time.Second,
		MaxInterval:         30 * time.Second,
		Multiplier:          2.0,
		RandomizationFactor: 0.1,
	}
}

// DefaultProducerOptions returns default producer options
func DefaultProducerOptions() *ProducerOptions {
	return &ProducerOptions{
		Timeout:       30 * time.Second,
		RetryConfig:   DefaultRetryConfig(),
		BatchSize:     100,
		DriverOptions: make(map[string]interface{}),
	}
}

// DefaultConsumerOptions returns default consumer options
func DefaultConsumerOptions() *ConsumerOptions {
	return &ConsumerOptions{
		Concurrency:   10,
		AutoAck:       false,
		RetryConfig:   DefaultRetryConfig(),
		DriverOptions: make(map[string]interface{}),
	}
}