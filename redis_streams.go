package bimawen

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStreamsDriver implements Redis Streams with consumer groups
type RedisStreamsDriver struct {
	client *redis.Client
	info   *URIInfo
	mu     sync.RWMutex
	closed bool
}

// NewRedisStreamsDriver creates a new Redis Streams driver instance
func NewRedisStreamsDriver(info *URIInfo) (*RedisStreamsDriver, error) {
	// Build Redis options
	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", info.Host, info.Port),
		Username: info.Username,
		Password: info.Password,
		DB:       info.Database,
	}

	// Apply additional options from URI query parameters
	if timeout, ok := info.Options["timeout"]; ok {
		if t, err := time.ParseDuration(timeout); err == nil {
			opts.DialTimeout = t
			opts.ReadTimeout = t
			opts.WriteTimeout = t
		}
	}

	// Create Redis client
	client := redis.NewClient(opts)

	driver := &RedisStreamsDriver{
		client: client,
		info:   info,
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return driver, nil
}

// NewProducer creates a new Redis Streams producer
func (d *RedisStreamsDriver) NewProducer(options *ProducerOptions) (Producer, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("driver is closed")
	}

	return NewRedisStreamsProducer(d.client, options)
}

// NewConsumer creates a new Redis Streams consumer
func (d *RedisStreamsDriver) NewConsumer(options *ConsumerOptions) (Consumer, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("driver is closed")
	}

	return NewRedisStreamsConsumer(d.client, options)
}

// HealthCheck checks if the connection is healthy
func (d *RedisStreamsDriver) HealthCheck(ctx context.Context) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return fmt.Errorf("driver is closed")
	}

	return d.client.Ping(ctx).Err()
}

// Close closes the driver and all its connections
func (d *RedisStreamsDriver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.closed = true
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}

// RedisStreamsProducer implements the Producer interface for Redis Streams
type RedisStreamsProducer struct {
	client          *redis.Client
	options         *ProducerOptions
	retryer         *Retryer
	circuitBreaker  *CircuitBreaker
	mu              sync.RWMutex
	closed          bool
}

// NewRedisStreamsProducer creates a new Redis Streams producer
func NewRedisStreamsProducer(client *redis.Client, options *ProducerOptions) (*RedisStreamsProducer, error) {
	producer := &RedisStreamsProducer{
		client:  client,
		options: options,
		retryer: NewRetryer(options.RetryConfig),
	}

	// Initialize circuit breaker if configured
	if options.CircuitBreakerConfig != nil && options.CircuitBreakerConfig.Enabled {
		producer.circuitBreaker = NewCircuitBreaker(options.CircuitBreakerConfig)
	}

	return producer, nil
}

// Send sends a message to Redis Stream
func (p *RedisStreamsProducer) Send(ctx context.Context, topic string, message *Message) error {
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
func (p *RedisStreamsProducer) SendAsync(ctx context.Context, topic string, message *Message) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)
		err := p.Send(ctx, topic, message)
		errChan <- err
	}()

	return errChan
}

// SendBatch sends multiple messages in a batch using pipeline
func (p *RedisStreamsProducer) SendBatch(ctx context.Context, topic string, messages []*Message) error {
	if p.circuitBreaker != nil {
		return p.circuitBreaker.Call(ctx, func(ctx context.Context) error {
			return p.retryer.Retry(ctx, func(ctx context.Context) error {
				return p.sendBatchMessages(ctx, topic, messages)
			})
		})
	}

	return p.retryer.Retry(ctx, func(ctx context.Context) error {
		return p.sendBatchMessages(ctx, topic, messages)
	})
}

// sendMessage sends a single message to Redis Stream
func (p *RedisStreamsProducer) sendMessage(ctx context.Context, topic string, message *Message) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return NewNonRetryableError(fmt.Errorf("producer is closed"))
	}

	// Create context with timeout if specified
	sendCtx := ctx
	if p.options.Timeout > 0 {
		var cancel context.CancelFunc
		sendCtx, cancel = context.WithTimeout(ctx, p.options.Timeout)
		defer cancel()
	}

	// Prepare stream entry fields
	fields := map[string]interface{}{
		"id":             message.ID,
		"body":           string(message.Body),
		"timestamp":      message.Timestamp.Unix(),
		"delivery_count": message.DeliveryCount,
		"priority":       message.Priority,
	}

	// Add headers as fields
	if message.Headers != nil {
		for k, v := range message.Headers {
			fields[fmt.Sprintf("header_%s", k)] = v
		}
	}

	// Add message to stream
	_, err := p.client.XAdd(sendCtx, &redis.XAddArgs{
		Stream: topic,
		ID:     "*", // Let Redis generate ID
		Values: fields,
	}).Result()

	if err != nil {
		return WrapRetryableError(fmt.Errorf("failed to send message to stream: %w", err))
	}

	// Set TTL on stream if specified
	if message.TTL > 0 {
		p.client.Expire(sendCtx, topic, message.TTL)
	}

	return nil
}

// sendBatchMessages sends multiple messages using pipeline
func (p *RedisStreamsProducer) sendBatchMessages(ctx context.Context, topic string, messages []*Message) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return NewNonRetryableError(fmt.Errorf("producer is closed"))
	}

	// Create context with timeout if specified
	sendCtx := ctx
	if p.options.Timeout > 0 {
		var cancel context.CancelFunc
		sendCtx, cancel = context.WithTimeout(ctx, p.options.Timeout)
		defer cancel()
	}

	pipe := p.client.Pipeline()

	for _, msg := range messages {
		// Prepare stream entry fields
		fields := map[string]interface{}{
			"id":             msg.ID,
			"body":           string(msg.Body),
			"timestamp":      msg.Timestamp.Unix(),
			"delivery_count": msg.DeliveryCount,
			"priority":       msg.Priority,
		}

		// Add headers as fields
		if msg.Headers != nil {
			for k, v := range msg.Headers {
				fields[fmt.Sprintf("header_%s", k)] = v
			}
		}

		// Add to pipeline
		pipe.XAdd(sendCtx, &redis.XAddArgs{
			Stream: topic,
			ID:     "*",
			Values: fields,
		})

		// Set TTL if specified
		if msg.TTL > 0 {
			pipe.Expire(sendCtx, topic, msg.TTL)
		}
	}

	// Execute pipeline
	_, err := pipe.Exec(sendCtx)
	if err != nil {
		return WrapRetryableError(fmt.Errorf("failed to execute batch: %w", err))
	}

	return nil
}

// GetClient returns the Redis client
// Implements RedisStreamsConnectionAccessor interface
func (p *RedisStreamsProducer) GetClient() *redis.Client {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	if p.closed {
		return nil
	}
	
	return p.client
}

// Close closes the producer
func (p *RedisStreamsProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	return nil
}

// RedisStreamsConsumer implements the Consumer interface for Redis Streams with consumer groups
type RedisStreamsConsumer struct {
	client         *redis.Client
	options        *ConsumerOptions
	retryer        *Retryer
	circuitBreaker *CircuitBreaker
	mu             sync.RWMutex
	closed         bool
}

// NewRedisStreamsConsumer creates a new Redis Streams consumer
func NewRedisStreamsConsumer(client *redis.Client, options *ConsumerOptions) (*RedisStreamsConsumer, error) {
	consumer := &RedisStreamsConsumer{
		client:  client,
		options: options,
		retryer: NewRetryer(options.RetryConfig),
	}

	// Initialize circuit breaker if configured
	if options.CircuitBreakerConfig != nil && options.CircuitBreakerConfig.Enabled {
		consumer.circuitBreaker = NewCircuitBreaker(options.CircuitBreakerConfig)
	}

	return consumer, nil
}

// Consume starts consuming messages from a stream
func (c *RedisStreamsConsumer) Consume(ctx context.Context, topic string, handler MessageHandler) error {
	return c.ConsumeWithOptions(ctx, topic, handler, nil)
}

// ConsumeWithOptions starts consuming with specific options including consumer groups
func (c *RedisStreamsConsumer) ConsumeWithOptions(ctx context.Context, topic string, handler MessageHandler, opts *ConsumeOptions) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("consumer is closed")
	}

	if opts != nil && opts.ConsumerGroup != "" {
		return c.consumeWithGroup(ctx, topic, handler, opts)
	} else {
		return c.consumeWithoutGroup(ctx, topic, handler, opts)
	}
}

// consumeWithGroup consumes messages using consumer group
func (c *RedisStreamsConsumer) consumeWithGroup(ctx context.Context, topic string, handler MessageHandler, opts *ConsumeOptions) error {
	consumerGroup := opts.ConsumerGroup
	consumerName := opts.ConsumerName
	if consumerName == "" {
		consumerName = fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	}

	// Ensure consumer group exists
	if err := c.ensureConsumerGroup(ctx, topic, consumerGroup, opts.StartPosition); err != nil {
		return fmt.Errorf("failed to ensure consumer group: %w", err)
	}

	// Create worker goroutines
	workerChan := make(chan redis.XMessage, c.options.Concurrency)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < c.options.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.workerWithGroup(ctx, topic, consumerGroup, consumerName, handler, workerChan)
		}()
	}

	// Start fetching messages
	go func() {
		defer close(workerChan)
		c.fetchMessagesWithGroup(ctx, topic, consumerGroup, consumerName, workerChan, opts)
	}()

	// Wait for context cancellation or workers to finish
	<-ctx.Done()
	wg.Wait()

	return ctx.Err()
}

// consumeWithoutGroup consumes messages without consumer group (simple stream reading)
func (c *RedisStreamsConsumer) consumeWithoutGroup(ctx context.Context, topic string, handler MessageHandler, opts *ConsumeOptions) error {
	// Implementation for simple stream reading without consumer groups
	startID := "0-0" // Start from beginning by default
	if opts != nil && opts.StartPosition != "" {
		if opts.StartPosition == "latest" {
			startID = "$"
		} else if opts.StartPosition == "earliest" {
			startID = "0-0"
		} else {
			startID = opts.StartPosition
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		blockTime := 1 * time.Second
		if opts != nil && opts.BlockTime > 0 {
			blockTime = opts.BlockTime
		}

		streams, err := c.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{topic, startID},
			Block:   blockTime,
			Count:   int64(c.options.Concurrency),
		}).Result()

		if err == redis.Nil {
			continue // No new messages
		}
		if err != nil {
			continue // Continue on error
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				// Process message
				c.processMessage(ctx, handler, msg)
				startID = msg.ID // Update start ID for next read
			}
		}
	}
}

// ensureConsumerGroup creates consumer group if it doesn't exist
func (c *RedisStreamsConsumer) ensureConsumerGroup(ctx context.Context, topic, group, startPosition string) error {
	startID := "0-0"
	if startPosition == "latest" {
		startID = "$"
	} else if startPosition == "earliest" {
		startID = "0-0"
	} else if startPosition != "" {
		startID = startPosition
	}

	// Try to create consumer group
	err := c.client.XGroupCreate(ctx, topic, group, startID).Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// fetchMessagesWithGroup fetches messages using consumer group
func (c *RedisStreamsConsumer) fetchMessagesWithGroup(ctx context.Context, topic, group, consumer string, workerChan chan<- redis.XMessage, opts *ConsumeOptions) {
	maxMessages := -1
	if opts != nil && opts.MaxMessages > 0 {
		maxMessages = opts.MaxMessages
	}

	messageCount := 0
	blockTime := 1 * time.Second
	if opts != nil && opts.BlockTime > 0 {
		blockTime = opts.BlockTime
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Check if we've reached the message limit
		if maxMessages > 0 && messageCount >= maxMessages {
			return
		}

		// Read messages from consumer group
		streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{topic, ">"},
			Block:    blockTime,
			Count:    1,
		}).Result()

		if err == redis.Nil {
			continue // No new messages
		}
		if err != nil {
			continue // Continue on error
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				select {
				case workerChan <- msg:
					messageCount++
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// workerWithGroup processes messages from the worker channel with consumer group acknowledgment
func (c *RedisStreamsConsumer) workerWithGroup(ctx context.Context, topic, group, consumer string, handler MessageHandler, workerChan <-chan redis.XMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-workerChan:
			if !ok {
				return
			}

			// Convert Redis stream message to our Message type
			message := c.convertStreamMessage(msg, topic)

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

			// Acknowledge message if processing succeeded
			if err == nil || (!IsRetryableError(err) && !IsCircuitBreakerError(err)) {
				c.client.XAck(ctx, topic, group, msg.ID)
			}
			// If error is retryable or circuit breaker error, leave message unacknowledged for retry
		}
	}
}

// processMessage processes a message without consumer group
func (c *RedisStreamsConsumer) processMessage(ctx context.Context, handler MessageHandler, msg redis.XMessage) {
	// Convert Redis stream message to our Message type
	message := c.convertStreamMessage(msg, "")

	// Handle message with circuit breaker and retry
	if c.circuitBreaker != nil {
		c.circuitBreaker.Call(ctx, func(ctx context.Context) error {
			return c.retryer.Retry(ctx, func(ctx context.Context) error {
				return handler(ctx, message)
			})
		})
	} else {
		// Fallback to direct call without circuit breaker
		c.retryer.Retry(ctx, func(ctx context.Context) error {
			return handler(ctx, message)
		})
	}
}

// convertStreamMessage converts Redis stream message to our Message type
func (c *RedisStreamsConsumer) convertStreamMessage(msg redis.XMessage, topic string) *Message {
	message := &Message{
		ID:      msg.ID,
		Topic:   topic,
		Headers: make(map[string]interface{}),
	}

	// Extract fields from stream message
	for field, value := range msg.Values {
		switch field {
		case "id":
			if id, ok := value.(string); ok {
				message.ID = id
			}
		case "body":
			if body, ok := value.(string); ok {
				message.Body = []byte(body)
			}
		case "timestamp":
			if ts, ok := value.(string); ok {
				if timestamp, err := time.Parse(time.RFC3339, ts); err == nil {
					message.Timestamp = timestamp
				}
			}
		case "delivery_count":
			if count, ok := value.(string); ok {
				if deliveryCount, err := fmt.Sscanf(count, "%d", &message.DeliveryCount); err == nil {
					_ = deliveryCount
				}
			}
		case "priority":
			if prio, ok := value.(string); ok {
				if priority, err := fmt.Sscanf(prio, "%d", &message.Priority); err == nil {
					_ = priority
				}
			}
		default:
			// Handle headers (prefixed with "header_")
			if strings.HasPrefix(field, "header_") {
				headerName := strings.TrimPrefix(field, "header_")
				message.Headers[headerName] = value
			}
		}
	}

	return message
}

// GetClient returns the Redis client
// Implements RedisStreamsConnectionAccessor interface
func (c *RedisStreamsConsumer) GetClient() *redis.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	if c.closed {
		return nil
	}
	
	return c.client
}

// Close closes the consumer
func (c *RedisStreamsConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	return nil
}

// NewDriver creates a new driver instance for Redis Streams 
func NewDriver(info *URIInfo) (Driver, error) {
	if info.DriverType == DriverRedisStreams {
		return NewRedisStreamsDriver(info)
	}
	return nil, fmt.Errorf("unsupported driver type for Redis Streams: %s", info.DriverType)
}

// Register Redis Streams driver with the default factory
func init() {
	DefaultDriverFactory.Register(DriverRedisStreams, func(info *URIInfo) (Driver, error) {
		return NewRedisStreamsDriver(info)
	})
}