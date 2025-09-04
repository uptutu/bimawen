package bimawen

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DeadLetterRetryStrategy defines the retry strategy type
type DeadLetterRetryStrategy string

const (
	// RetryStrategyFixed uses fixed delay between retries
	RetryStrategyFixed DeadLetterRetryStrategy = "fixed"
	// RetryStrategyLinear increases delay linearly with attempt count
	RetryStrategyLinear DeadLetterRetryStrategy = "linear"
	// RetryStrategyExponential uses exponential backoff
	RetryStrategyExponential DeadLetterRetryStrategy = "exponential"
)

// DeadLetterConfig contains dead letter queue configuration
type DeadLetterConfig struct {
	// Enabled enables dead letter queue functionality
	Enabled bool
	
	// QueueName is the name of the dead letter queue
	QueueName string
	
	// ExchangeName is the name of the dead letter exchange
	ExchangeName string
	
	// RoutingKey for dead letter messages
	RoutingKey string
	
	// TTL for messages in the dead letter queue (0 means no TTL)
	TTL time.Duration
	
	// MaxLength maximum number of messages in dead letter queue (0 means no limit)
	MaxLength int
	
	// MaxRetries maximum number of times a message can be retried before going to DLQ
	MaxRetries int
	
	// RetryDelay delay between retries (base delay for exponential/linear strategies)
	RetryDelay time.Duration
	
	// RetryStrategy defines how retry delays are calculated
	RetryStrategy DeadLetterRetryStrategy
	
	// RetryMultiplier multiplier for exponential/linear strategies (default: 2.0)
	RetryMultiplier float64
	
	// MaxRetryDelay maximum delay between retries (for exponential/linear strategies)
	MaxRetryDelay time.Duration
	
	// AutoRequeue whether to automatically requeue messages from DLQ after a certain time
	AutoRequeue bool
	
	// AutoRequeueDelay delay before auto-requeue attempts
	AutoRequeueDelay time.Duration
	
	// AutoRequeueMaxAttempts maximum attempts for auto-requeue (0 = infinite)
	AutoRequeueMaxAttempts int
	
	// AlertThreshold threshold for DLQ message count to trigger alerts (0 = disabled)
	AlertThreshold int
	
	// AlertCallback callback function for DLQ alerts
	AlertCallback func(stats *DeadLetterStats)
	
	// PreserveOriginalHeaders whether to preserve original message headers in DLQ
	PreserveOriginalHeaders bool
	
	// CustomHeaders custom headers to add to dead letter messages
	CustomHeaders map[string]interface{}
}

// DefaultDeadLetterConfig returns default dead letter configuration
func DefaultDeadLetterConfig(topicName string) *DeadLetterConfig {
	return &DeadLetterConfig{
		Enabled:                 false,
		QueueName:              fmt.Sprintf("%s.dlq", topicName),
		ExchangeName:           fmt.Sprintf("%s.dlx", topicName),
		RoutingKey:             "dead",
		TTL:                    24 * time.Hour, // Keep dead letters for 24 hours
		MaxLength:              10000,          // Max 10k messages in DLQ
		MaxRetries:             3,
		RetryDelay:             30 * time.Second,
		RetryStrategy:          RetryStrategyFixed,
		RetryMultiplier:        2.0,
		MaxRetryDelay:          10 * time.Minute,
		AutoRequeue:            false,
		AutoRequeueDelay:       1 * time.Hour,
		AutoRequeueMaxAttempts: 0, // Infinite
		AlertThreshold:         1000, // Alert when 1000 messages in DLQ
		AlertCallback:          nil,
		PreserveOriginalHeaders: true,
		CustomHeaders:          make(map[string]interface{}),
	}
}

// calculateRetryDelay calculates the retry delay based on the retry strategy and attempt count
func (config *DeadLetterConfig) calculateRetryDelay(attemptCount int) time.Duration {
	if config.RetryDelay <= 0 || attemptCount <= 0 {
		return 0
	}
	
	var delay time.Duration
	
	switch config.RetryStrategy {
	case RetryStrategyFixed:
		delay = config.RetryDelay
	case RetryStrategyLinear:
		delay = time.Duration(int64(config.RetryDelay) * int64(attemptCount))
	case RetryStrategyExponential:
		multiplier := config.RetryMultiplier
		if multiplier <= 0 {
			multiplier = 2.0
		}
		// For exponential: delay = baseDelay * multiplier^(attemptCount-1)
		factor := 1.0
		for i := 1; i < attemptCount; i++ {
			factor *= multiplier
		}
		delay = time.Duration(float64(config.RetryDelay) * factor)
	default:
		delay = config.RetryDelay
	}
	
	// Apply maximum delay limit
	if config.MaxRetryDelay > 0 && delay > config.MaxRetryDelay {
		delay = config.MaxRetryDelay
	}
	
	return delay
}

// DeadLetterMessage represents a message in the dead letter queue
type DeadLetterMessage struct {
	// Original message
	OriginalMessage *Message
	
	// Reason why the message was dead lettered
	Reason string
	
	// FailureCount number of times this message has failed
	FailureCount int
	
	// LastFailure timestamp of the last failure
	LastFailure time.Time
	
	// FirstFailure timestamp of the first failure
	FirstFailure time.Time
	
	// OriginalQueue the queue where the message originally failed
	OriginalQueue string
	
	// StackTrace error stack trace (if available)
	StackTrace string
}

// DeadLetterHandler handles dead letter queue operations
type DeadLetterHandler struct {
	config   *DeadLetterConfig
	channel  *amqp.Channel
	exchange string
	queue    string
}

// NewDeadLetterHandler creates a new dead letter handler
func NewDeadLetterHandler(channel *amqp.Channel, config *DeadLetterConfig) (*DeadLetterHandler, error) {
	if !config.Enabled {
		return nil, fmt.Errorf("dead letter queue is not enabled")
	}
	
	// Validate configuration
	if err := config.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("invalid dead letter configuration: %w", err)
	}
	
	handler := &DeadLetterHandler{
		config:   config,
		channel:  channel,
		exchange: config.ExchangeName,
		queue:    config.QueueName,
	}
	
	// Set up dead letter infrastructure
	if err := handler.setupInfrastructure(); err != nil {
		return nil, fmt.Errorf("failed to setup dead letter infrastructure: %w", err)
	}
	
	return handler, nil
}

// setupInfrastructure sets up the dead letter exchange and queue
func (h *DeadLetterHandler) setupInfrastructure() error {
	// Declare dead letter exchange
	err := h.channel.ExchangeDeclare(
		h.exchange, // name
		"direct",   // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare dead letter exchange: %w", err)
	}
	
	// Prepare queue arguments
	args := make(amqp.Table)
	if h.config.TTL > 0 {
		args["x-message-ttl"] = int64(h.config.TTL / time.Millisecond)
	}
	if h.config.MaxLength > 0 {
		args["x-max-length"] = int32(h.config.MaxLength)
		args["x-overflow"] = "drop-head" // Drop oldest messages when queue is full
	}
	
	// Declare dead letter queue
	_, err = h.channel.QueueDeclare(
		h.queue, // name
		true,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		args,    // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare dead letter queue: %w", err)
	}
	
	// Bind dead letter queue to exchange
	err = h.channel.QueueBind(
		h.queue,           // queue name
		h.config.RoutingKey, // routing key
		h.exchange,        // exchange
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to bind dead letter queue: %w", err)
	}
	
	return nil
}

// SendToDeadLetter sends a message to the dead letter queue
func (h *DeadLetterHandler) SendToDeadLetter(ctx context.Context, message *Message, reason string, failureCount int, originalQueue string) error {
	if !h.config.Enabled {
		return fmt.Errorf("dead letter queue is disabled")
	}
	
	// Create dead letter message
	dlMessage := &DeadLetterMessage{
		OriginalMessage: message,
		Reason:          reason,
		FailureCount:    failureCount,
		LastFailure:     time.Now(),
		OriginalQueue:   originalQueue,
	}
	
	// Set first failure time if this is the first failure
	if failureCount == 1 {
		dlMessage.FirstFailure = dlMessage.LastFailure
	}
	
	// Serialize the dead letter message
	serializer := NewJSONSerializer()
	dlData, err := serializer.Serialize(dlMessage)
	if err != nil {
		return fmt.Errorf("failed to serialize dead letter message: %w", err)
	}
	
	// Prepare headers with dead letter information
	headers := make(amqp.Table)
	
	// Add original message headers if configured to preserve them
	if h.config.PreserveOriginalHeaders && message.Headers != nil {
		for k, v := range message.Headers {
			headers[k] = v
		}
	}
	
	// Add custom headers if configured
	if h.config.CustomHeaders != nil {
		for k, v := range h.config.CustomHeaders {
			headers[k] = v
		}
	}
	
	// Add standard dead letter headers
	headers["x-death-reason"] = reason
	headers["x-death-count"] = failureCount
	headers["x-death-time"] = dlMessage.LastFailure.Unix()
	headers["x-original-queue"] = originalQueue
	headers["x-first-death-time"] = dlMessage.FirstFailure.Unix()
	
	// Publish to dead letter queue
	err = h.channel.PublishWithContext(
		ctx,
		h.exchange,          // exchange
		h.config.RoutingKey, // routing key
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         dlData,
			Headers:      headers,
			Timestamp:    time.Now(),
			DeliveryMode: amqp.Persistent,
		},
	)
	
	if err != nil {
		return fmt.Errorf("failed to publish to dead letter queue: %w", err)
	}
	
	return nil
}

// GetDeadLetterStats returns statistics about the dead letter queue
func (h *DeadLetterHandler) GetDeadLetterStats(ctx context.Context) (*DeadLetterStats, error) {
	if !h.config.Enabled {
		return nil, fmt.Errorf("dead letter queue is disabled")
	}
	
	// Inspect the dead letter queue
	queue, err := h.channel.QueueInspect(h.queue)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect dead letter queue: %w", err)
	}
	
	stats := &DeadLetterStats{
		QueueName:     h.queue,
		MessageCount:  queue.Messages,
		ConsumerCount: queue.Consumers,
	}
	
	// Check alert threshold and trigger callback if needed
	if h.config.AlertThreshold > 0 && stats.MessageCount >= h.config.AlertThreshold {
		if h.config.AlertCallback != nil {
			go h.config.AlertCallback(stats) // Run in goroutine to avoid blocking
		}
	}
	
	return stats, nil
}

// PurgeDeadLetterQueue purges all messages from the dead letter queue
func (h *DeadLetterHandler) PurgeDeadLetterQueue(ctx context.Context) (int, error) {
	if !h.config.Enabled {
		return 0, fmt.Errorf("dead letter queue is disabled")
	}
	
	count, err := h.channel.QueuePurge(h.queue, false)
	if err != nil {
		return 0, fmt.Errorf("failed to purge dead letter queue: %w", err)
	}
	
	return count, nil
}

// RequeueFromDeadLetter requeues a message from dead letter queue back to original queue
func (h *DeadLetterHandler) RequeueFromDeadLetter(ctx context.Context, originalQueue string, maxMessages int) (int, error) {
	if !h.config.Enabled {
		return 0, fmt.Errorf("dead letter queue is disabled")
	}
	
	requeuCount := 0
	
	for i := 0; i < maxMessages; i++ {
		// Get message from dead letter queue
		delivery, ok, err := h.channel.Get(h.queue, false)
		if err != nil {
			return requeuCount, fmt.Errorf("failed to get message from dead letter queue: %w", err)
		}
		
		if !ok {
			// No more messages
			break
		}
		
		// Deserialize dead letter message
		var dlMessage DeadLetterMessage
		serializer := NewJSONSerializer()
		err = serializer.Deserialize(delivery.Body, &dlMessage)
		if err != nil {
			// Reject malformed message
			delivery.Nack(false, false)
			continue
		}
		
		// Only requeue to the specified queue
		if dlMessage.OriginalQueue != originalQueue {
			// Requeue to dead letter queue
			delivery.Nack(false, true)
			continue
		}
		
		// Publish back to original queue
		err = h.channel.PublishWithContext(
			ctx,
			"",           // exchange
			originalQueue, // routing key
			false,        // mandatory
			false,        // immediate
			amqp.Publishing{
				ContentType:  "application/json",
				Body:         dlMessage.OriginalMessage.Body,
				Headers:      dlMessage.OriginalMessage.Headers,
				Timestamp:    time.Now(),
				DeliveryMode: amqp.Persistent,
			},
		)
		
		if err != nil {
			// Requeue to dead letter queue
			delivery.Nack(false, true)
			return requeuCount, fmt.Errorf("failed to requeue message: %w", err)
		}
		
		// Acknowledge the dead letter message
		delivery.Ack(false)
		requeuCount++
	}
	
	return requeuCount, nil
}

// StartAutoRequeue starts the auto-requeue process for the dead letter queue
// This runs in a separate goroutine and periodically checks for messages to requeue
func (h *DeadLetterHandler) StartAutoRequeue(ctx context.Context, originalQueue string) {
	if !h.config.Enabled || !h.config.AutoRequeue || h.config.AutoRequeueDelay <= 0 {
		return
	}
	
	go func() {
		ticker := time.NewTicker(h.config.AutoRequeueDelay)
		defer ticker.Stop()
		
		attemptCount := 0
		
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Check if we've exceeded max attempts
				if h.config.AutoRequeueMaxAttempts > 0 {
					attemptCount++
					if attemptCount > h.config.AutoRequeueMaxAttempts {
						return
					}
				}
				
				// Try to requeue some messages
				_, err := h.RequeueFromDeadLetter(ctx, originalQueue, 10) // Requeue up to 10 messages
				if err != nil {
					// Log error - in a real implementation you might want proper logging
					continue
				}
			}
		}
	}()
}

// ValidateConfig validates the dead letter queue configuration
func (config *DeadLetterConfig) ValidateConfig() error {
	if !config.Enabled {
		return nil // No validation needed for disabled config
	}
	
	if config.QueueName == "" {
		return fmt.Errorf("queue name is required when dead letter queue is enabled")
	}
	
	if config.ExchangeName == "" {
		return fmt.Errorf("exchange name is required when dead letter queue is enabled")
	}
	
	if config.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}
	
	if config.RetryDelay < 0 {
		return fmt.Errorf("retry delay cannot be negative")
	}
	
	if config.RetryMultiplier <= 0 {
		return fmt.Errorf("retry multiplier must be positive")
	}
	
	if config.MaxRetryDelay < 0 {
		return fmt.Errorf("max retry delay cannot be negative")
	}
	
	if config.MaxRetryDelay > 0 && config.MaxRetryDelay < config.RetryDelay {
		return fmt.Errorf("max retry delay cannot be less than base retry delay")
	}
	
	if config.AutoRequeueDelay < 0 {
		return fmt.Errorf("auto requeue delay cannot be negative")
	}
	
	if config.AutoRequeueMaxAttempts < 0 {
		return fmt.Errorf("auto requeue max attempts cannot be negative")
	}
	
	if config.AlertThreshold < 0 {
		return fmt.Errorf("alert threshold cannot be negative")
	}
	
	// Validate retry strategy
	switch config.RetryStrategy {
	case RetryStrategyFixed, RetryStrategyLinear, RetryStrategyExponential:
		// Valid strategies
	default:
		return fmt.Errorf("invalid retry strategy: %s", config.RetryStrategy)
	}
	
	return nil
}

// DeadLetterStats contains statistics about the dead letter queue
type DeadLetterStats struct {
	QueueName     string
	MessageCount  int
	ConsumerCount int
}

// RabbitMQConsumerWithDLQ extends RabbitMQConsumer with dead letter queue support
type RabbitMQConsumerWithDLQ struct {
	pool      *ConnectionPool
	options   *ConsumerOptions
	retryer   *Retryer
	dlHandler *DeadLetterHandler
	dlConfig  *DeadLetterConfig
	mu        sync.RWMutex
	closed    bool
}

// NewRabbitMQConsumerWithDLQ creates a new RabbitMQ consumer with dead letter queue support
func NewRabbitMQConsumerWithDLQ(pool *ConnectionPool, options *ConsumerOptions, dlConfig *DeadLetterConfig) (*RabbitMQConsumerWithDLQ, error) {
	consumer := &RabbitMQConsumerWithDLQ{
		pool:     pool,
		options:  options,
		retryer:  NewRetryer(options.RetryConfig),
		dlConfig: dlConfig,
	}
	
	// Set up dead letter handler if enabled
	if dlConfig != nil && dlConfig.Enabled {
		ch, err := pool.GetChannel()
		if err != nil {
			return nil, fmt.Errorf("failed to get channel for dead letter setup: %w", err)
		}
		defer pool.ReturnChannel(ch)
		
		dlHandler, err := NewDeadLetterHandler(ch, dlConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create dead letter handler: %w", err)
		}
		consumer.dlHandler = dlHandler
	}
	
	return consumer, nil
}

// ConsumeWithOptions starts consuming with specific options and dead letter queue support
func (c *RabbitMQConsumerWithDLQ) ConsumeWithOptions(ctx context.Context, topic string, handler MessageHandler, opts *ConsumeOptions) error {
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

	// Prepare queue arguments for dead letter queue
	queueArgs := make(amqp.Table)
	if c.dlConfig != nil && c.dlConfig.Enabled {
		queueArgs["x-dead-letter-exchange"] = c.dlConfig.ExchangeName
		queueArgs["x-dead-letter-routing-key"] = c.dlConfig.RoutingKey
	}

	// Declare main queue (with dead letter configuration)
	_, err = ch.QueueDeclare(
		topic, // queue name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		queueArgs, // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Start consuming
	msgs, err := ch.Consume(
		topic, // queue
		"",    // consumer
		false, // auto-ack (we'll handle ack manually)
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
					DeliveryCount: 1,
					Priority:      d.Priority,
				}

				// Copy headers
				for k, v := range d.Headers {
					message.Headers[k] = v
				}

				// Handle message with dead letter support
				err := c.handleMessageWithDLQ(ctx, d, message, handler, topic)
				if err != nil {
					// Log error - actual logging would depend on your logging framework
					// For now we just ensure the message is properly handled
				}
			}(delivery)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	return ctx.Err()
}

// handleMessageWithDLQ handles a message with dead letter queue support
func (c *RabbitMQConsumerWithDLQ) handleMessageWithDLQ(ctx context.Context, delivery amqp.Delivery, message *Message, handler MessageHandler, topic string) error {
	// Get current retry count from headers
	retryCount := 0
	if count, ok := message.Headers["x-retry-count"]; ok {
		if countInt, ok := count.(int); ok {
			retryCount = countInt
		}
	}

	// Try to process the message
	err := handler(ctx, message)
	
	if err != nil {
		retryCount++
		
		// Check if we should send to dead letter queue
		if c.dlConfig != nil && c.dlConfig.Enabled && retryCount >= c.dlConfig.MaxRetries {
			// Send to dead letter queue
			if c.dlHandler != nil {
				dlErr := c.dlHandler.SendToDeadLetter(ctx, message, err.Error(), retryCount, topic)
				if dlErr != nil {
					// Failed to send to DLQ, reject without requeue
					delivery.Nack(false, false)
					return fmt.Errorf("failed to send to dead letter queue: %w", dlErr)
				}
			}
			// Acknowledge the message as it's been handled by DLQ
			delivery.Ack(false)
			return nil
		}

		// Retry the message
		if IsRetryableError(err) {
			// Add retry count to headers for next attempt
			newHeaders := make(amqp.Table)
			for k, v := range delivery.Headers {
				newHeaders[k] = v
			}
			newHeaders["x-retry-count"] = retryCount
			newHeaders["x-retry-time"] = time.Now().Unix()

			// Wait for retry delay if configured
			if c.dlConfig != nil && c.dlConfig.RetryDelay > 0 {
				delay := c.dlConfig.calculateRetryDelay(retryCount)
				if delay > 0 {
					time.Sleep(delay)
				}
			}

			// Republish with updated headers
			err = c.republishMessage(ctx, delivery, newHeaders, topic)
			if err != nil {
				// Failed to republish, nack without requeue
				delivery.Nack(false, false)
				return fmt.Errorf("failed to republish message for retry: %w", err)
			}
			
			// Acknowledge original message
			delivery.Ack(false)
			return nil
		} else {
			// Non-retryable error, send directly to DLQ or reject
			if c.dlConfig != nil && c.dlConfig.Enabled && c.dlHandler != nil {
				dlErr := c.dlHandler.SendToDeadLetter(ctx, message, err.Error(), retryCount, topic)
				if dlErr == nil {
					delivery.Ack(false)
					return nil
				}
			}
			// Reject without requeue
			delivery.Nack(false, false)
			return err
		}
	}

	// Message processed successfully
	delivery.Ack(false)
	return nil
}

// republishMessage republishes a message for retry
func (c *RabbitMQConsumerWithDLQ) republishMessage(ctx context.Context, delivery amqp.Delivery, headers amqp.Table, topic string) error {
	// Get a new channel for publishing
	ch, err := c.pool.GetChannel()
	if err != nil {
		return err
	}
	defer c.pool.ReturnChannel(ch)

	// Republish the message
	return ch.PublishWithContext(
		ctx,
		"",    // exchange
		topic, // routing key (queue name)
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  delivery.ContentType,
			Body:         delivery.Body,
			Headers:      headers,
			DeliveryMode: delivery.DeliveryMode,
			Priority:     delivery.Priority,
			Timestamp:    time.Now(),
		},
	)
}

// Consume implements the Consumer interface
func (c *RabbitMQConsumerWithDLQ) Consume(ctx context.Context, topic string, handler MessageHandler) error {
	return c.ConsumeWithOptions(ctx, topic, handler, nil)
}

// Close closes the consumer
func (c *RabbitMQConsumerWithDLQ) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	return nil
}

// GetDeadLetterStats returns dead letter queue statistics
func (c *RabbitMQConsumerWithDLQ) GetDeadLetterStats(ctx context.Context) (*DeadLetterStats, error) {
	if c.dlHandler == nil {
		return nil, fmt.Errorf("dead letter queue is not enabled")
	}
	return c.dlHandler.GetDeadLetterStats(ctx)
}

// PurgeDeadLetterQueue purges the dead letter queue
func (c *RabbitMQConsumerWithDLQ) PurgeDeadLetterQueue(ctx context.Context) (int, error) {
	if c.dlHandler == nil {
		return 0, fmt.Errorf("dead letter queue is not enabled")
	}
	return c.dlHandler.PurgeDeadLetterQueue(ctx)
}

// RequeueFromDeadLetter requeues messages from dead letter queue
func (c *RabbitMQConsumerWithDLQ) RequeueFromDeadLetter(ctx context.Context, originalQueue string, maxMessages int) (int, error) {
	if c.dlHandler == nil {
		return 0, fmt.Errorf("dead letter queue is not enabled")
	}
	return c.dlHandler.RequeueFromDeadLetter(ctx, originalQueue, maxMessages)
}