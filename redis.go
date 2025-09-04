package bimawen

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisDriver implements the Driver interface for Redis
type RedisDriver struct {
	client *redis.Client
	info   *URIInfo
	mu     sync.RWMutex
	closed bool
}

// NewRedisDriver creates a new Redis driver instance
func NewRedisDriver(info *URIInfo) (*RedisDriver, error) {
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

	if poolSize, ok := info.Options["pool_size"]; ok {
		if size, err := strconv.Atoi(poolSize); err == nil && size > 0 {
			opts.PoolSize = size
		}
	}

	if minIdleConns, ok := info.Options["min_idle_conns"]; ok {
		if conns, err := strconv.Atoi(minIdleConns); err == nil && conns >= 0 {
			opts.MinIdleConns = conns
		}
	}

	// Create Redis client
	client := redis.NewClient(opts)

	driver := &RedisDriver{
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

// NewProducer creates a new Redis producer
func (d *RedisDriver) NewProducer(options *ProducerOptions) (Producer, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("driver is closed")
	}

	return NewRedisProducer(d.client, options)
}

// NewConsumer creates a new Redis consumer
func (d *RedisDriver) NewConsumer(options *ConsumerOptions) (Consumer, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("driver is closed")
	}

	return NewRedisConsumer(d.client, options)
}

// HealthCheck checks if the connection is healthy
func (d *RedisDriver) HealthCheck(ctx context.Context) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return fmt.Errorf("driver is closed")
	}

	return d.client.Ping(ctx).Err()
}

// Close closes the driver and all its connections
func (d *RedisDriver) Close() error {
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

// RedisProducer implements the Producer interface for Redis
type RedisProducer struct {
	client   *redis.Client
	options  *ProducerOptions
	retryer  *Retryer
	mu       sync.RWMutex
	closed   bool
}

// NewRedisProducer creates a new Redis producer
func NewRedisProducer(client *redis.Client, options *ProducerOptions) (*RedisProducer, error) {
	producer := &RedisProducer{
		client:  client,
		options: options,
		retryer: NewRetryer(options.RetryConfig),
	}

	return producer, nil
}

// Send sends a message synchronously using Redis list
func (p *RedisProducer) Send(ctx context.Context, topic string, message *Message) error {
	return p.retryer.Retry(ctx, func(ctx context.Context) error {
		return p.sendMessage(ctx, topic, message)
	})
}

// SendAsync sends a message asynchronously
func (p *RedisProducer) SendAsync(ctx context.Context, topic string, message *Message) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)
		err := p.Send(ctx, topic, message)
		errChan <- err
	}()

	return errChan
}

// SendBatch sends multiple messages in a batch using pipeline
func (p *RedisProducer) SendBatch(ctx context.Context, topic string, messages []*Message) error {
	return p.retryer.Retry(ctx, func(ctx context.Context) error {
		pipe := p.client.Pipeline()

		for _, msg := range messages {
			// Serialize the message
			data, err := p.serializeMessage(msg)
			if err != nil {
				return NewNonRetryableError(fmt.Errorf("failed to serialize message: %w", err))
			}

			// Add to pipeline
			pipe.LPush(ctx, topic, data)
			
			// Set TTL if specified
			if msg.TTL > 0 {
				pipe.Expire(ctx, topic, msg.TTL)
			}
		}

		// Execute pipeline
		_, err := pipe.Exec(ctx)
		if err != nil {
			return WrapRetryableError(fmt.Errorf("failed to execute batch: %w", err))
		}

		return nil
	})
}

// sendMessage sends a single message
func (p *RedisProducer) sendMessage(ctx context.Context, topic string, message *Message) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return NewNonRetryableError(fmt.Errorf("producer is closed"))
	}

	// Serialize the message
	data, err := p.serializeMessage(message)
	if err != nil {
		return NewNonRetryableError(fmt.Errorf("failed to serialize message: %w", err))
	}

	// Create context with timeout if specified
	sendCtx := ctx
	if p.options.Timeout > 0 {
		var cancel context.CancelFunc
		sendCtx, cancel = context.WithTimeout(ctx, p.options.Timeout)
		defer cancel()
	}

	// Handle delayed messages
	if message.Delay > 0 {
		// Use sorted set for delayed messages
		delayedKey := fmt.Sprintf("%s:delayed", topic)
		score := float64(time.Now().Add(message.Delay).Unix())
		
		err = p.client.ZAdd(sendCtx, delayedKey, redis.Z{
			Score:  score,
			Member: data,
		}).Err()
	} else {
		// Send immediately using list
		err = p.client.LPush(sendCtx, topic, data).Err()
	}

	if err != nil {
		return WrapRetryableError(fmt.Errorf("failed to send message: %w", err))
	}

	// Set TTL if specified
	if message.TTL > 0 {
		p.client.Expire(sendCtx, topic, message.TTL)
	}

	return nil
}

// serializeMessage serializes a message to JSON format
func (p *RedisProducer) serializeMessage(message *Message) (string, error) {
	// Create a serializable message structure
	msgData := map[string]interface{}{
		"id":             message.ID,
		"body":           string(message.Body),
		"headers":        message.Headers,
		"timestamp":      message.Timestamp.Unix(),
		"delivery_count": message.DeliveryCount,
		"priority":       message.Priority,
	}

	data, err := p.options.Serializer.Serialize(msgData)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// GetClient returns the Redis client
// Implements RedisConnectionAccessor interface
func (p *RedisProducer) GetClient() *redis.Client {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	if p.closed {
		return nil
	}
	
	return p.client
}

// Close closes the producer
func (p *RedisProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	return nil
}

// RedisConsumer implements the Consumer interface for Redis
type RedisConsumer struct {
	client   *redis.Client
	options  *ConsumerOptions
	retryer  *Retryer
	mu       sync.RWMutex
	closed   bool
}

// NewRedisConsumer creates a new Redis consumer
func NewRedisConsumer(client *redis.Client, options *ConsumerOptions) (*RedisConsumer, error) {
	consumer := &RedisConsumer{
		client:  client,
		options: options,
		retryer: NewRetryer(options.RetryConfig),
	}

	return consumer, nil
}

// Consume starts consuming messages from a topic
func (c *RedisConsumer) Consume(ctx context.Context, topic string, handler MessageHandler) error {
	return c.ConsumeWithOptions(ctx, topic, handler, nil)
}

// ConsumeWithOptions starts consuming with specific options
func (c *RedisConsumer) ConsumeWithOptions(ctx context.Context, topic string, handler MessageHandler, opts *ConsumeOptions) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("consumer is closed")
	}

	// Create worker goroutines
	workerChan := make(chan string, c.options.Concurrency)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < c.options.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.worker(ctx, topic, handler, workerChan)
		}()
	}

	// Start fetching messages
	go func() {
		defer close(workerChan)
		c.fetchMessages(ctx, topic, workerChan, opts)
	}()

	// Wait for context cancellation or workers to finish
	<-ctx.Done()
	wg.Wait()

	return ctx.Err()
}

// fetchMessages fetches messages from Redis and sends them to workers
func (c *RedisConsumer) fetchMessages(ctx context.Context, topic string, workerChan chan<- string, opts *ConsumeOptions) {
	maxMessages := -1
	if opts != nil && opts.MaxMessages > 0 {
		maxMessages = opts.MaxMessages
	}

	messageCount := 0

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

		// First check for delayed messages that are ready
		c.processDelayedMessages(ctx, topic)

		// Try to pop a message from the list
		result, err := c.client.BRPop(ctx, time.Second, topic).Result()
		if err != nil {
			if err == redis.Nil {
				continue // No message available, retry
			}
			if err == context.Canceled || err == context.DeadlineExceeded {
				return
			}
			// Log error and continue
			continue
		}

		if len(result) >= 2 {
			select {
			case workerChan <- result[1]:
				messageCount++
			case <-ctx.Done():
				return
			}
		}
	}
}

// processDelayedMessages moves ready delayed messages to the main queue
func (c *RedisConsumer) processDelayedMessages(ctx context.Context, topic string) {
	delayedKey := fmt.Sprintf("%s:delayed", topic)
	now := float64(time.Now().Unix())

	// Get messages that are ready to be processed
	result, err := c.client.ZRangeByScoreWithScores(ctx, delayedKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%f", now),
		Count: 100, // Process up to 100 delayed messages at once
	}).Result()

	if err != nil || len(result) == 0 {
		return
	}

	// Move messages to the main queue
	pipe := c.client.Pipeline()
	for _, z := range result {
		pipe.LPush(ctx, topic, z.Member)
		pipe.ZRem(ctx, delayedKey, z.Member)
	}
	pipe.Exec(ctx)
}

// worker processes messages from the worker channel
func (c *RedisConsumer) worker(ctx context.Context, topic string, handler MessageHandler, workerChan <-chan string) {
	for {
		select {
		case <-ctx.Done():
			return
		case msgData, ok := <-workerChan:
			if !ok {
				return
			}

			// Deserialize message
			message, err := c.deserializeMessage(msgData)
			if err != nil {
				// Log error and continue
				continue
			}

			message.Topic = topic

			// Handle message with retry
			err = c.retryer.Retry(ctx, func(ctx context.Context) error {
				return handler(ctx, message)
			})

			// Note: Redis doesn't have built-in acknowledgment like RabbitMQ,
			// so we rely on the retry mechanism in the handler
			if err != nil {
				// Could implement dead letter queue here
				// For now, we just log and continue
			}
		}
	}
}

// deserializeMessage deserializes a message from JSON format
func (c *RedisConsumer) deserializeMessage(data string) (*Message, error) {
	var msgData map[string]interface{}
	
	err := c.options.Serializer.Deserialize([]byte(data), &msgData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %w", err)
	}

	message := &Message{
		Headers: make(map[string]interface{}),
	}

	// Extract fields
	if id, ok := msgData["id"].(string); ok {
		message.ID = id
	}

	if body, ok := msgData["body"].(string); ok {
		message.Body = []byte(body)
	}

	if headers, ok := msgData["headers"].(map[string]interface{}); ok {
		message.Headers = headers
	}

	if timestamp, ok := msgData["timestamp"].(float64); ok {
		message.Timestamp = time.Unix(int64(timestamp), 0)
	}

	if deliveryCount, ok := msgData["delivery_count"].(float64); ok {
		message.DeliveryCount = int(deliveryCount)
	}

	if priority, ok := msgData["priority"].(float64); ok {
		message.Priority = uint8(priority)
	}

	return message, nil
}

// GetClient returns the Redis client
// Implements RedisConnectionAccessor interface
func (c *RedisConsumer) GetClient() *redis.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	if c.closed {
		return nil
	}
	
	return c.client
}

// Close closes the consumer
func (c *RedisConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	return nil
}

// Register Redis driver with the default factory
func init() {
	DefaultDriverFactory.Register(DriverRedis, func(info *URIInfo) (Driver, error) {
		return NewRedisDriver(info)
	})
}