package bimawen

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublishConfirm represents a publish confirmation
type PublishConfirm struct {
	DeliveryTag uint64
	Ack         bool
	Error       error
}

// ConfirmChannel wraps an AMQP channel with confirmation support
type ConfirmChannel struct {
	channel       *amqp.Channel
	confirmations chan amqp.Confirmation
	returns       chan amqp.Return
	mu            sync.RWMutex
	closed        bool
	nextSeqNo     uint64
	pending       map[uint64]*PendingConfirm
}

// PendingConfirm represents a message waiting for confirmation
type PendingConfirm struct {
	SeqNo     uint64
	ResultCh  chan PublishConfirm
	Message   *Message
	Topic     string
	Timestamp time.Time
}

// NewConfirmChannel creates a new confirm channel
func NewConfirmChannel(channel *amqp.Channel) (*ConfirmChannel, error) {
	// Enable publish confirms
	if err := channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("failed to enable publisher confirms: %w", err)
	}

	cc := &ConfirmChannel{
		channel:       channel,
		confirmations: channel.NotifyPublish(make(chan amqp.Confirmation, 1000)),
		returns:       channel.NotifyReturn(make(chan amqp.Return, 1000)),
		nextSeqNo:     1,
		pending:       make(map[uint64]*PendingConfirm),
	}

	// Start confirmation handler
	go cc.handleConfirmations()
	go cc.handleReturns()

	return cc, nil
}

// PublishWithConfirm publishes a message and waits for confirmation
func (cc *ConfirmChannel) PublishWithConfirm(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*PublishConfirm, error) {
	cc.mu.Lock()
	if cc.closed {
		cc.mu.Unlock()
		return nil, fmt.Errorf("channel is closed")
	}

	seqNo := cc.nextSeqNo
	cc.nextSeqNo++

	// Create pending confirmation
	pending := &PendingConfirm{
		SeqNo:     seqNo,
		ResultCh:  make(chan PublishConfirm, 1),
		Timestamp: time.Now(),
	}
	cc.pending[seqNo] = pending
	cc.mu.Unlock()

	// Publish the message
	err := cc.channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		// Remove from pending on immediate error
		cc.mu.Lock()
		delete(cc.pending, seqNo)
		cc.mu.Unlock()
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}

	// Wait for confirmation with timeout
	select {
	case confirm := <-pending.ResultCh:
		return &confirm, nil
	case <-ctx.Done():
		// Clean up pending confirmation
		cc.mu.Lock()
		delete(cc.pending, seqNo)
		cc.mu.Unlock()
		return nil, ctx.Err()
	case <-time.After(30 * time.Second):
		// Timeout
		cc.mu.Lock()
		delete(cc.pending, seqNo)
		cc.mu.Unlock()
		return nil, fmt.Errorf("confirmation timeout for sequence %d", seqNo)
	}
}

// handleConfirmations processes confirmation messages
func (cc *ConfirmChannel) handleConfirmations() {
	for confirmation := range cc.confirmations {
		cc.mu.Lock()
		if pending, ok := cc.pending[confirmation.DeliveryTag]; ok {
			delete(cc.pending, confirmation.DeliveryTag)
			
			result := PublishConfirm{
				DeliveryTag: confirmation.DeliveryTag,
				Ack:         confirmation.Ack,
			}
			
			if !confirmation.Ack {
				result.Error = fmt.Errorf("message nacked by broker")
			}
			
			// Send result to waiting goroutine
			select {
			case pending.ResultCh <- result:
			default:
				// Channel might be closed, ignore
			}
		}
		cc.mu.Unlock()
	}
}

// handleReturns processes returned messages (undeliverable messages)
func (cc *ConfirmChannel) handleReturns() {
	for returnMsg := range cc.returns {
		// Find the corresponding pending confirmation by matching properties
		// This is a best-effort approach since RabbitMQ doesn't provide sequence numbers for returns
		cc.mu.Lock()
		for seqNo, pending := range cc.pending {
			// Try to match based on timing (messages published recently)
			if time.Since(pending.Timestamp) < 5*time.Second {
				delete(cc.pending, seqNo)
				
				result := PublishConfirm{
					DeliveryTag: uint64(seqNo),
					Ack:         false,
					Error: fmt.Errorf("message returned: code=%d, text=%s, exchange=%s, routing_key=%s",
						returnMsg.ReplyCode, returnMsg.ReplyText, returnMsg.Exchange, returnMsg.RoutingKey),
				}
				
				select {
				case pending.ResultCh <- result:
				default:
				}
				break
			}
		}
		cc.mu.Unlock()
	}
}

// Close closes the confirm channel
func (cc *ConfirmChannel) Close() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.closed {
		return nil
	}

	cc.closed = true
	
	// Notify all pending confirmations of closure
	for seqNo, pending := range cc.pending {
		result := PublishConfirm{
			DeliveryTag: uint64(seqNo),
			Ack:         false,
			Error:       fmt.Errorf("channel closed"),
		}
		
		select {
		case pending.ResultCh <- result:
		default:
		}
	}
	
	// Clear pending map
	cc.pending = make(map[uint64]*PendingConfirm)
	
	return cc.channel.Close()
}

// RabbitMQProducerWithConfirms extends RabbitMQProducer with publisher confirms
type RabbitMQProducerWithConfirms struct {
	pool            *ConnectionPool
	options         *ProducerOptions
	retryer         *Retryer
	confirmChannels map[*amqp.Channel]*ConfirmChannel
	channelMu       sync.RWMutex
	mu              sync.RWMutex
	closed          bool
	enableConfirms  bool
}

// NewRabbitMQProducerWithConfirms creates a new RabbitMQ producer with publisher confirms
func NewRabbitMQProducerWithConfirms(pool *ConnectionPool, options *ProducerOptions, enableConfirms bool) (*RabbitMQProducerWithConfirms, error) {
	producer := &RabbitMQProducerWithConfirms{
		pool:            pool,
		options:         options,
		retryer:         NewRetryer(options.RetryConfig),
		confirmChannels: make(map[*amqp.Channel]*ConfirmChannel),
		enableConfirms:  enableConfirms,
	}

	return producer, nil
}

// getConfirmChannel gets or creates a confirm channel for the given channel
func (p *RabbitMQProducerWithConfirms) getConfirmChannel(ch *amqp.Channel) (*ConfirmChannel, error) {
	p.channelMu.RLock()
	if cc, ok := p.confirmChannels[ch]; ok {
		p.channelMu.RUnlock()
		return cc, nil
	}
	p.channelMu.RUnlock()

	p.channelMu.Lock()
	defer p.channelMu.Unlock()

	// Double-check after acquiring write lock
	if cc, ok := p.confirmChannels[ch]; ok {
		return cc, nil
	}

	// Create new confirm channel
	cc, err := NewConfirmChannel(ch)
	if err != nil {
		return nil, err
	}

	p.confirmChannels[ch] = cc
	return cc, nil
}

// Send implements the Producer interface
func (p *RabbitMQProducerWithConfirms) Send(ctx context.Context, topic string, message *Message) error {
	if p.enableConfirms {
		confirm, err := p.SendWithConfirm(ctx, topic, message)
		if err != nil {
			return err
		}
		if !confirm.Ack {
			return fmt.Errorf("message not confirmed: %v", confirm.Error)
		}
		return nil
	}
	
	// Fallback to regular send without confirms
	return p.sendMessageWithoutConfirm(ctx, topic, message)
}

// SendAsync implements the Producer interface
func (p *RabbitMQProducerWithConfirms) SendAsync(ctx context.Context, topic string, message *Message) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)
		err := p.Send(ctx, topic, message)
		errChan <- err
	}()

	return errChan
}

// SendBatch implements the Producer interface
func (p *RabbitMQProducerWithConfirms) SendBatch(ctx context.Context, topic string, messages []*Message) error {
	for _, msg := range messages {
		if err := p.Send(ctx, topic, msg); err != nil {
			return err
		}
	}
	return nil
}

// sendMessageWithoutConfirm sends a message without waiting for confirmation
func (p *RabbitMQProducerWithConfirms) sendMessageWithoutConfirm(ctx context.Context, topic string, message *Message) error {
	return p.retryer.Retry(ctx, func(ctx context.Context) error {
		return p.sendMessageRegular(ctx, topic, message)
	})
}

// sendMessageRegular sends a message using regular publish (without confirms)
func (p *RabbitMQProducerWithConfirms) sendMessageRegular(ctx context.Context, topic string, message *Message) error {
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

	// Prepare message
	var body []byte
	if message.Body != nil {
		body = message.Body
	} else {
		return NewNonRetryableError(fmt.Errorf("message body is required"))
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

	// Publish message
	err = ch.PublishWithContext(
		publishCtx,
		"",    // exchange
		topic, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  p.options.Serializer.ContentType(),
			Body:         body,
			Headers:      headers,
			Priority:     message.Priority,
			Timestamp:    message.Timestamp,
			DeliveryMode: amqp.Persistent, // make message persistent
			Expiration:   formatTTL(message.TTL),
		},
	)

	if err != nil {
		return WrapRetryableError(fmt.Errorf("failed to publish message: %w", err))
	}

	return nil
}
func (p *RabbitMQProducerWithConfirms) SendWithConfirm(ctx context.Context, topic string, message *Message) (*PublishConfirm, error) {
	if !p.enableConfirms {
		return nil, fmt.Errorf("publisher confirms are not enabled")
	}

	var result *PublishConfirm
	err := p.retryer.Retry(ctx, func(ctx context.Context) error {
		confirm, err := p.sendMessageWithConfirm(ctx, topic, message)
		if err != nil {
			return err
		}
		
		if !confirm.Ack {
			return NewRetryableError(confirm.Error)
		}
		
		result = confirm
		return nil
	})
	
	return result, err
}

// sendMessageWithConfirm sends a single message with confirmation
func (p *RabbitMQProducerWithConfirms) sendMessageWithConfirm(ctx context.Context, topic string, message *Message) (*PublishConfirm, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, NewNonRetryableError(fmt.Errorf("producer is closed"))
	}

	// Get a channel from the pool
	ch, err := p.pool.GetChannel()
	if err != nil {
		return nil, WrapRetryableError(fmt.Errorf("failed to get channel: %w", err))
	}
	defer p.pool.ReturnChannel(ch)

	// Get confirm channel
	cc, err := p.getConfirmChannel(ch)
	if err != nil {
		return nil, WrapRetryableError(fmt.Errorf("failed to get confirm channel: %w", err))
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
		return nil, WrapRetryableError(fmt.Errorf("failed to declare queue: %w", err))
	}

	// Prepare message
	var body []byte
	if message.Body != nil {
		body = message.Body
	} else {
		return nil, NewNonRetryableError(fmt.Errorf("message body is required"))
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

	// Publish with confirmation
	publishing := amqp.Publishing{
		ContentType:  p.options.Serializer.ContentType(),
		Body:         body,
		Headers:      headers,
		Priority:     message.Priority,
		Timestamp:    message.Timestamp,
		DeliveryMode: amqp.Persistent, // make message persistent
		Expiration:   formatTTL(message.TTL),
	}

	return cc.PublishWithConfirm(ctx, "", topic, false, false, publishing)
}

// Close closes the producer with confirms
func (p *RabbitMQProducerWithConfirms) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	
	// Close all confirm channels
	p.channelMu.Lock()
	for _, cc := range p.confirmChannels {
		cc.Close()
	}
	p.confirmChannels = make(map[*amqp.Channel]*ConfirmChannel)
	p.channelMu.Unlock()

	return nil
}