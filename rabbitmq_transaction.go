package bimawen

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQTransaction implements Transaction interface for RabbitMQ
type RabbitMQTransaction struct {
	*BaseTransaction
	driver  *RabbitMQDriver
	channel *amqp.Channel
	txMode  bool // Whether channel is in transaction mode
}

// NewRabbitMQTransaction creates a new RabbitMQ transaction
func NewRabbitMQTransaction(driver *RabbitMQDriver, options *TransactionOptions) (*RabbitMQTransaction, error) {
	base := NewBaseTransaction(options)
	
	return &RabbitMQTransaction{
		BaseTransaction: base,
		driver:          driver,
	}, nil
}

// Begin starts the RabbitMQ transaction
func (rt *RabbitMQTransaction) Begin(ctx context.Context) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	if rt.state != TransactionIdle {
		return NewTransactionError(rt.id, rt.state, "transaction not in idle state", nil)
	}
	
	// Get a channel from the connection pool
	var err error
	if rt.driver.pool != nil {
		rt.channel, err = rt.driver.pool.GetChannel()
	} else {
		return NewTransactionError(rt.id, rt.state, "no connection pool available", nil)
	}
	
	if err != nil {
		return NewTransactionError(rt.id, rt.state, "failed to create channel", err)
	}
	
	// Enable transaction mode on the channel
	err = rt.channel.Tx()
	if err != nil {
		rt.channel.Close()
		return NewTransactionError(rt.id, rt.state, "failed to enable transaction mode", err)
	}
	
	rt.txMode = true
	rt.setState(TransactionActive)
	rt.startTime = rt.startTime // Set in setState callback
	
	// Setup timeout if configured
	if rt.options.Timeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, rt.options.Timeout)
		rt.timeoutCancel = cancel
		
		go rt.handleTimeout(timeoutCtx)
	}
	
	return nil
}

// handleTimeout handles transaction timeout
func (rt *RabbitMQTransaction) handleTimeout(ctx context.Context) {
	<-ctx.Done()
	
	if ctx.Err() == context.DeadlineExceeded && rt.options.AutoRollback {
		// Auto-rollback on timeout
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*rt.options.Timeout)
		defer cancel()
		
		rt.Rollback(rollbackCtx)
	}
}

// Send sends a message within the transaction
func (rt *RabbitMQTransaction) Send(ctx context.Context, topic string, message *Message) error {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	
	if rt.state != TransactionActive {
		return NewTransactionError(rt.id, rt.state, "transaction not active", nil)
	}
	
	// Serialize the message
	serializer := DefaultSerializerRegistry.GetDefault()
	
	body, err := serializer.Serialize(message)
	if err != nil {
		return NewTransactionError(rt.id, rt.state, "failed to serialize message", err)
	}
	
	// Prepare headers
	headers := make(amqp.Table)
	if message.Headers != nil {
		for k, v := range message.Headers {
			headers[k] = v
		}
	}
	headers["content-type"] = serializer.ContentType()
	headers["message-id"] = message.ID
	headers["timestamp"] = message.Timestamp.Unix()
	headers["delivery-count"] = message.DeliveryCount
	headers["priority"] = message.Priority
	
	// Prepare AMQP message
	publishing := amqp.Publishing{
		ContentType:   serializer.ContentType(),
		Body:          body,
		Headers:       headers,
		MessageId:     message.ID,
		Timestamp:     message.Timestamp,
		DeliveryMode:  2, // Persistent
		Priority:      message.Priority,
	}
	
	if message.TTL > 0 {
		publishing.Expiration = fmt.Sprintf("%d", message.TTL.Milliseconds())
	}
	
	// Publish the message within transaction
	err = rt.channel.PublishWithContext(
		ctx,
		"",    // exchange (use default)
		topic,
		false, // mandatory
		false, // immediate
		publishing,
	)
	
	if err != nil {
		return NewTransactionError(rt.id, rt.state, "failed to publish message in transaction", err)
	}
	
	// Record the operation
	rt.addOperation(TransactionOperation{
		Type:    "send",
		Topic:   topic,
		Message: message,
	})
	
	return nil
}

// SendBatch sends multiple messages within the transaction
func (rt *RabbitMQTransaction) SendBatch(ctx context.Context, topic string, messages []*Message) error {
	for _, message := range messages {
		if err := rt.Send(ctx, topic, message); err != nil {
			return err
		}
	}
	
	// Record batch operation
	rt.addOperation(TransactionOperation{
		Type:     "send_batch",
		Topic:    topic,
		Messages: messages,
		Metadata: map[string]interface{}{
			"batch_size": len(messages),
		},
	})
	
	return nil
}

// Prepare prepares the transaction for commit (RabbitMQ doesn't support 2PC natively)
func (rt *RabbitMQTransaction) Prepare(ctx context.Context) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	if rt.state != TransactionActive {
		return NewTransactionError(rt.id, rt.state, "transaction not active", nil)
	}
	
	if !rt.options.TwoPhaseCommit {
		return NewTransactionError(rt.id, rt.state, "two-phase commit not enabled", nil)
	}
	
	// RabbitMQ doesn't have native 2PC support, but we can simulate prepare state
	rt.setState(TransactionPrepared)
	
	return nil
}

// Commit commits the transaction
func (rt *RabbitMQTransaction) Commit(ctx context.Context) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	if rt.state != TransactionActive && rt.state != TransactionPrepared {
		return NewTransactionError(rt.id, rt.state, "transaction not in committable state", nil)
	}
	
	if !rt.txMode {
		return NewTransactionError(rt.id, rt.state, "transaction mode not enabled", nil)
	}
	
	// Commit the transaction
	err := rt.channel.TxCommit()
	if err != nil {
		rt.setState(TransactionFailed)
		return NewTransactionError(rt.id, rt.state, "failed to commit transaction", err)
	}
	
	rt.setState(TransactionCommitted)
	
	// Record commit operation
	rt.addOperation(TransactionOperation{
		Type: "commit",
		Metadata: map[string]interface{}{
			"operation_count": len(rt.operations),
			"duration_ms":     rt.Duration().Milliseconds(),
		},
	})
	
	return nil
}

// Rollback rolls back the transaction
func (rt *RabbitMQTransaction) Rollback(ctx context.Context) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	if rt.state != TransactionActive && rt.state != TransactionPrepared {
		return NewTransactionError(rt.id, rt.state, "transaction not in rollback-able state", nil)
	}
	
	if !rt.txMode {
		return NewTransactionError(rt.id, rt.state, "transaction mode not enabled", nil)
	}
	
	// Rollback the transaction
	err := rt.channel.TxRollback()
	if err != nil {
		rt.setState(TransactionFailed)
		return NewTransactionError(rt.id, rt.state, "failed to rollback transaction", err)
	}
	
	rt.setState(TransactionRolledBack)
	
	// Record rollback operation
	rt.addOperation(TransactionOperation{
		Type: "rollback",
		Metadata: map[string]interface{}{
			"operation_count": len(rt.operations),
			"duration_ms":     rt.Duration().Milliseconds(),
		},
	})
	
	return nil
}

// Close closes the transaction and releases resources
func (rt *RabbitMQTransaction) Close() error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	// Cancel timeout if active
	if rt.timeoutCancel != nil {
		rt.timeoutCancel()
		rt.timeoutCancel = nil
	}
	
	// Close the channel if open
	if rt.channel != nil {
		// Return channel to pool or close it
		if rt.driver.pool != nil {
			rt.driver.pool.ReturnChannel(rt.channel)
		} else {
			rt.channel.Close()
		}
		rt.channel = nil
	}
	
	rt.txMode = false
	
	return nil
}