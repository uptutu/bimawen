package bimawen

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQDriver implements the Driver interface for RabbitMQ with connection pooling
type RabbitMQDriver struct {
	pool   *ConnectionPool
	info   *URIInfo
	mu     sync.RWMutex
	closed bool
}

// NewRabbitMQDriver creates a new RabbitMQ driver instance with connection pooling and TLS support
func NewRabbitMQDriver(info *URIInfo) (*RabbitMQDriver, error) {
	// Build connection URL
	var connURL string
	if info.Username != "" && info.Password != "" {
		connURL = fmt.Sprintf("amqp://%s:%s@%s:%d%s",
			info.Username, info.Password, info.Host, info.Port, info.VHost)
	} else {
		connURL = fmt.Sprintf("amqp://%s:%d%s",
			info.Host, info.Port, info.VHost)
	}

	// Create connection pool config
	poolConfig := DefaultConnectionPoolConfig()
	
	// Apply URI options to pool config
	if maxConn, ok := info.Options["max_connections"]; ok {
		if val, err := parseInt(maxConn); err == nil && val > 0 {
			poolConfig.MaxConnections = val
		}
	}
	if maxCh, ok := info.Options["max_channels"]; ok {
		if val, err := parseInt(maxCh); err == nil && val > 0 {
			poolConfig.MaxChannels = val
		}
	}

	// Parse TLS options from URI
	tlsConfig := ParseTLSOptionsFromURI(info.Options)
	poolConfig.TLSConfig = tlsConfig

	// Adjust URL scheme for TLS
	if tlsConfig.Enabled {
		connURL = replaceScheme(connURL, "amqps")
	}

	pool, err := NewConnectionPool(connURL, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	driver := &RabbitMQDriver{
		pool: pool,
		info: info,
	}

	return driver, nil
}

// NewProducer creates a new RabbitMQ producer
func (d *RabbitMQDriver) NewProducer(options *ProducerOptions) (Producer, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("driver is closed")
	}

	// Check if publisher confirms are enabled
	if options.EnableConfirms {
		return NewRabbitMQProducerWithConfirms(d.pool, options, true)
	}

	return NewRabbitMQProducer(d.pool, options)
}

// NewConsumer creates a new RabbitMQ consumer
func (d *RabbitMQDriver) NewConsumer(options *ConsumerOptions) (Consumer, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("driver is closed")
	}

	// Check if dead letter queue is enabled
	if options.DeadLetterConfig != nil && options.DeadLetterConfig.Enabled {
		return NewRabbitMQConsumerWithDLQ(d.pool, options, options.DeadLetterConfig)
	}

	return NewRabbitMQConsumer(d.pool, options)
}

// HealthCheck checks if the connection pool is healthy
func (d *RabbitMQDriver) HealthCheck(ctx context.Context) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return fmt.Errorf("driver is closed")
	}

	return d.pool.HealthCheck(ctx)
}

// Close closes the driver and connection pool
func (d *RabbitMQDriver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.closed = true
	if d.pool != nil {
		return d.pool.Close()
	}
	return nil
}

// RabbitMQProducer implements the Producer interface for RabbitMQ with connection pooling
type RabbitMQProducer struct {
	pool           *ConnectionPool
	options        *ProducerOptions
	retryer        *Retryer
	circuitBreaker *CircuitBreaker
	mu             sync.RWMutex
	closed         bool
}

// NewRabbitMQProducer creates a new RabbitMQ producer
func NewRabbitMQProducer(pool *ConnectionPool, options *ProducerOptions) (*RabbitMQProducer, error) {
	producer := &RabbitMQProducer{
		pool:    pool,
		options: options,
		retryer: NewRetryer(options.RetryConfig),
	}
	
	// Initialize circuit breaker if configured
	if options.CircuitBreakerConfig != nil && options.CircuitBreakerConfig.Enabled {
		producer.circuitBreaker = NewCircuitBreaker(options.CircuitBreakerConfig)
	}

	return producer, nil
}

// Send sends a message synchronously
func (p *RabbitMQProducer) Send(ctx context.Context, topic string, message *Message) error {
	// Use circuit breaker if configured
	if p.circuitBreaker != nil {
		return p.circuitBreaker.Call(ctx, func(ctx context.Context) error {
			return p.retryer.Retry(ctx, func(ctx context.Context) error {
				return p.sendMessage(ctx, topic, message)
			})
		})
	}
	
	// Fallback to direct call without circuit breaker
	return p.retryer.Retry(ctx, func(ctx context.Context) error {
		return p.sendMessage(ctx, topic, message)
	})
}

// SendAsync sends a message asynchronously
func (p *RabbitMQProducer) SendAsync(ctx context.Context, topic string, message *Message) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)
		err := p.Send(ctx, message.Topic, message)
		errChan <- err
	}()

	return errChan
}

// SendBatch sends multiple messages in a batch
func (p *RabbitMQProducer) SendBatch(ctx context.Context, topic string, messages []*Message) error {
	return p.retryer.Retry(ctx, func(ctx context.Context) error {
		for _, msg := range messages {
			if err := p.sendMessage(ctx, topic, msg); err != nil {
				return err
			}
		}
		return nil
	})
}

// sendMessage sends a single message using pooled connections
func (p *RabbitMQProducer) sendMessage(ctx context.Context, topic string, message *Message) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return NewNonRetryableError(fmt.Errorf("producer is closed"))
	}

	// Get a channel from the pool
	ch, err := p.pool.GetChannel()
	if err != nil {
		return WrapRetryableError(fmt.Errorf("failed to get channel: %w", err))
	}
	defer p.pool.ReturnChannel(ch)

	// Serialize message body if it's not already bytes
	var body []byte

	if message.Body != nil {
		body = message.Body
	} else {
		return NewNonRetryableError(fmt.Errorf("message body is required"))
	}

	// Declare queue (idempotent)
	_, err = ch.QueueDeclare(
		topic, // queue name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return WrapRetryableError(fmt.Errorf("failed to declare queue: %w", err))
	}

	// Prepare headers
	headers := make(amqp.Table)
	if message.Headers != nil {
		for k, v := range message.Headers {
			headers[k] = v
		}
	}
	headers["timestamp"] = message.Timestamp.Unix()
	headers["message_id"] = message.ID

	// Create publishing context with timeout
	publishCtx := ctx
	if p.options.Timeout > 0 {
		var cancel context.CancelFunc
		publishCtx, cancel = context.WithTimeout(ctx, p.options.Timeout)
		defer cancel()
	}

	// Get RabbitMQ-specific publish options from DriverOptions
	exchange := ""
	mandatory := false
	immediate := false
	
	if p.options.DriverOptions != nil {
		if v, ok := p.options.DriverOptions[RabbitMQExchange]; ok {
			if s, ok := v.(string); ok {
				exchange = s
			}
		}
		if v, ok := p.options.DriverOptions[RabbitMQMandatory]; ok {
			if b, ok := v.(bool); ok {
				mandatory = b
			}
		}
		if v, ok := p.options.DriverOptions[RabbitMQImmediate]; ok {
			if b, ok := v.(bool); ok {
				immediate = b
			}
		}
	}

	// Publish message
	err = ch.PublishWithContext(
		publishCtx,
		exchange, // configurable exchange
		topic,    // routing key
		mandatory, // configurable mandatory
		immediate, // configurable immediate
		amqp.Publishing{
			ContentType:   p.options.Serializer.ContentType(),
			Body:          body,
			Headers:       headers,
			Priority:      message.Priority,
			Timestamp:     message.Timestamp,
			DeliveryMode:  amqp.Persistent, // make message persistent
			Expiration:    formatTTL(message.TTL),
		},
	)

	if err != nil {
		return WrapRetryableError(fmt.Errorf("failed to publish message: %w", err))
	}

	return nil
}

// GetChannel returns a channel from the connection pool
// Implements RabbitMQConnectionAccessor interface
func (p *RabbitMQProducer) GetChannel() *amqp.Channel {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	if p.closed || p.pool == nil {
		return nil
	}
	
	channel, err := p.pool.GetChannel()
	if err != nil {
		return nil
	}
	
	return channel
}

// GetConnection returns a connection from the connection pool
// Implements RabbitMQConnectionAccessor interface  
func (p *RabbitMQProducer) GetConnection() *amqp.Connection {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	if p.closed || p.pool == nil {
		return nil
	}
	
	conn, err := p.pool.GetConnection()
	if err != nil {
		return nil
	}
	
	return conn
}

// Close closes the producer
func (p *RabbitMQProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	return nil
}

// RabbitMQConsumer implements the Consumer interface for RabbitMQ with connection pooling
type RabbitMQConsumer struct {
	pool           *ConnectionPool
	options        *ConsumerOptions
	retryer        *Retryer
	circuitBreaker *CircuitBreaker
	mu             sync.RWMutex
	closed         bool
}

// NewRabbitMQConsumer creates a new RabbitMQ consumer
func NewRabbitMQConsumer(pool *ConnectionPool, options *ConsumerOptions) (*RabbitMQConsumer, error) {
	consumer := &RabbitMQConsumer{
		pool:    pool,
		options: options,
		retryer: NewRetryer(options.RetryConfig),
	}

	// Initialize circuit breaker if configured
	if options.CircuitBreakerConfig != nil && options.CircuitBreakerConfig.Enabled {
		consumer.circuitBreaker = NewCircuitBreaker(options.CircuitBreakerConfig)
	}

	return consumer, nil
}

// Consume starts consuming messages from a topic
func (c *RabbitMQConsumer) Consume(ctx context.Context, topic string, handler MessageHandler) error {
	return c.ConsumeWithOptions(ctx, topic, handler, nil)
}

// ConsumeWithOptions starts consuming with specific options using pooled connections
func (c *RabbitMQConsumer) ConsumeWithOptions(ctx context.Context, topic string, handler MessageHandler, opts *ConsumeOptions) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("consumer is closed")
	}

	// Get a channel from the pool
	ch, err := c.pool.GetChannel()
	if err != nil {
		return fmt.Errorf("failed to get channel: %w", err)
	}
	defer c.pool.ReturnChannel(ch)

	// Set QoS for fair dispatch
	err = ch.Qos(
		c.options.Concurrency, // prefetch count
		0,                     // prefetch size
		false,                 // global
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Declare queue (idempotent)
	_, err = ch.QueueDeclare(
		topic, // queue name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Start consuming
	msgs, err := ch.Consume(
		topic, // queue
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	// Handle messages in goroutines
	semaphore := make(chan struct{}, c.options.Concurrency)

	go func() {
		for delivery := range msgs {
			select {
			case <-ctx.Done():
				return
			case semaphore <- struct{}{}: // acquire semaphore
			}

			go func(d amqp.Delivery) {
				defer func() { <-semaphore }() // release semaphore

				// Convert AMQP delivery to our Message type
				message := &Message{
					ID:            d.MessageId,
					Topic:         topic,
					Body:          d.Body,
					Headers:       make(map[string]interface{}),
					Timestamp:     d.Timestamp,
					DeliveryCount: 1, // RabbitMQ doesn't expose delivery count directly
					Priority:      d.Priority,
				}

				// Copy headers
				for k, v := range d.Headers {
					message.Headers[k] = v
				}

				// Handle message with circuit breaker and retry
				var err error
				if c.circuitBreaker != nil {
					err = c.circuitBreaker.Call(ctx, func(ctx context.Context) error {
						return c.retryer.Retry(ctx, func(ctx context.Context) error {
							return handler(ctx, message)
						})
					})
				} else {
					// Fallback to direct call without circuit breaker
					err = c.retryer.Retry(ctx, func(ctx context.Context) error {
						return handler(ctx, message)
					})
				}

				// Handle acknowledgment
				if err != nil {
					// Handle circuit breaker errors specially
					if IsCircuitBreakerError(err) {
						// Circuit breaker is open, reject without requeue to avoid overwhelming
						d.Nack(false, false)
					} else if IsRetryableError(err) {
						// Reject and requeue for retryable errors
						d.Nack(false, true) // requeue
					} else {
						d.Nack(false, false) // don't requeue
					}
				} else {
					if !c.options.AutoAck {
						d.Ack(false)
					}
				}
			}(delivery)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	return ctx.Err()
}

// GetChannel returns a channel from the connection pool
// Implements RabbitMQConnectionAccessor interface
func (c *RabbitMQConsumer) GetChannel() *amqp.Channel {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	if c.closed || c.pool == nil {
		return nil
	}
	
	channel, err := c.pool.GetChannel()
	if err != nil {
		return nil
	}
	
	return channel
}

// GetConnection returns a connection from the connection pool
// Implements RabbitMQConnectionAccessor interface
func (c *RabbitMQConsumer) GetConnection() *amqp.Connection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	if c.closed || c.pool == nil {
		return nil
	}
	
	conn, err := c.pool.GetConnection()
	if err != nil {
		return nil
	}
	
	return conn
}

// Close closes the consumer
func (c *RabbitMQConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	return nil
}

// parseInt safely converts string to int
func parseInt(s string) (int, error) {
	return strconv.Atoi(s)
}

// replaceScheme replaces the scheme in a URL
func replaceScheme(url, newScheme string) string {
	if idx := strings.Index(url, "://"); idx != -1 {
		return newScheme + url[idx:]
	}
	return url
}

// formatTTL formats TTL duration to RabbitMQ expiration format
func formatTTL(ttl time.Duration) string {
	if ttl <= 0 {
		return ""
	}
	return fmt.Sprintf("%d", int64(ttl/time.Millisecond))
}

// Register RabbitMQ driver with the default factory
func init() {
	DefaultDriverFactory.Register(DriverRabbitMQ, func(info *URIInfo) (Driver, error) {
		return NewRabbitMQDriver(info)
	})
}