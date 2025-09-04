package bimawen

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisTransaction implements Transaction interface for Redis
type RedisTransaction struct {
	*BaseTransaction
	driver    *RedisDriver
	client    *redis.Client
	txPipe    redis.Pipeliner
	commands  []redis.Cmder
}

// NewRedisTransaction creates a new Redis transaction
func NewRedisTransaction(driver *RedisDriver, options *TransactionOptions) (*RedisTransaction, error) {
	base := NewBaseTransaction(options)
	
	return &RedisTransaction{
		BaseTransaction: base,
		driver:          driver,
		commands:        make([]redis.Cmder, 0),
	}, nil
}

// Begin starts the Redis transaction
func (rt *RedisTransaction) Begin(ctx context.Context) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	if rt.state != TransactionIdle {
		return NewTransactionError(rt.id, rt.state, "transaction not in idle state", nil)
	}
	
	// Get Redis client (use connection pool if available)
	if rt.driver.client != nil {
		rt.client = rt.driver.client
	} else {
		return NewTransactionError(rt.id, rt.state, "no Redis client available", nil)
	}
	
	// Start Redis transaction pipeline
	rt.txPipe = rt.client.TxPipeline()
	
	rt.setState(TransactionActive)
	rt.startTime = rt.startTime
	
	// Setup timeout if configured
	if rt.options.Timeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, rt.options.Timeout)
		rt.timeoutCancel = cancel
		
		go rt.handleTimeout(timeoutCtx)
	}
	
	return nil
}

// handleTimeout handles transaction timeout
func (rt *RedisTransaction) handleTimeout(ctx context.Context) {
	<-ctx.Done()
	
	if ctx.Err() == context.DeadlineExceeded && rt.options.AutoRollback {
		// Auto-rollback on timeout
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*rt.options.Timeout)
		defer cancel()
		
		rt.Rollback(rollbackCtx)
	}
}

// Send sends a message within the transaction
func (rt *RedisTransaction) Send(ctx context.Context, topic string, message *Message) error {
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
	
	// Add to Redis transaction pipeline
	var cmd redis.Cmder
	
	if message.Delay > 0 {
		// Use sorted set for delayed messages
		score := float64(rt.startTime.Add(message.Delay).Unix())
		cmd = rt.txPipe.ZAdd(ctx, fmt.Sprintf("delayed:%s", topic), redis.Z{
			Score:  score,
			Member: string(body),
		})
	} else {
		// Regular message to list
		cmd = rt.txPipe.LPush(ctx, topic, string(body))
	}
	
	rt.commands = append(rt.commands, cmd)
	
	// Set TTL if specified
	if message.TTL > 0 {
		ttlCmd := rt.txPipe.Expire(ctx, topic, message.TTL)
		rt.commands = append(rt.commands, ttlCmd)
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
func (rt *RedisTransaction) SendBatch(ctx context.Context, topic string, messages []*Message) error {
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

// Prepare prepares the transaction for commit
func (rt *RedisTransaction) Prepare(ctx context.Context) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	if rt.state != TransactionActive {
		return NewTransactionError(rt.id, rt.state, "transaction not active", nil)
	}
	
	if !rt.options.TwoPhaseCommit {
		return NewTransactionError(rt.id, rt.state, "two-phase commit not enabled", nil)
	}
	
	// Redis doesn't have native 2PC, but we can simulate prepare state
	rt.setState(TransactionPrepared)
	
	return nil
}

// Commit commits the transaction
func (rt *RedisTransaction) Commit(ctx context.Context) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	if rt.state != TransactionActive && rt.state != TransactionPrepared {
		return NewTransactionError(rt.id, rt.state, "transaction not in committable state", nil)
	}
	
	if rt.txPipe == nil {
		return NewTransactionError(rt.id, rt.state, "transaction pipeline not initialized", nil)
	}
	
	// Execute the transaction pipeline
	results, err := rt.txPipe.Exec(ctx)
	if err != nil {
		rt.setState(TransactionFailed)
		return NewTransactionError(rt.id, rt.state, "failed to execute transaction pipeline", err)
	}
	
	// Check if all commands succeeded
	for i, result := range results {
		if result.Err() != nil {
			rt.setState(TransactionFailed)
			return NewTransactionError(rt.id, rt.state, 
				fmt.Sprintf("command %d failed in transaction", i), result.Err())
		}
	}
	
	rt.setState(TransactionCommitted)
	
	// Record commit operation
	rt.addOperation(TransactionOperation{
		Type: "commit",
		Metadata: map[string]interface{}{
			"command_count":   len(rt.commands),
			"operation_count": len(rt.operations),
			"duration_ms":     rt.Duration().Milliseconds(),
		},
	})
	
	return nil
}

// Rollback rolls back the transaction
func (rt *RedisTransaction) Rollback(ctx context.Context) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	if rt.state != TransactionActive && rt.state != TransactionPrepared {
		return NewTransactionError(rt.id, rt.state, "transaction not in rollback-able state", nil)
	}
	
	// Redis pipelines can be discarded by not executing them
	if rt.txPipe != nil {
		rt.txPipe.Discard()
	}
	
	rt.setState(TransactionRolledBack)
	
	// Record rollback operation
	rt.addOperation(TransactionOperation{
		Type: "rollback",
		Metadata: map[string]interface{}{
			"command_count":   len(rt.commands),
			"operation_count": len(rt.operations),
			"duration_ms":     rt.Duration().Milliseconds(),
		},
	})
	
	return nil
}

// Close closes the transaction and releases resources
func (rt *RedisTransaction) Close() error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	// Cancel timeout if active
	if rt.timeoutCancel != nil {
		rt.timeoutCancel()
		rt.timeoutCancel = nil
	}
	
	// Clean up pipeline
	if rt.txPipe != nil {
		rt.txPipe = nil
	}
	
	// Clear client reference (we don't need to return it since it's the driver's client)
	rt.client = nil
	
	// Clear commands
	rt.commands = rt.commands[:0]
	
	return nil
}

// RedisStreamsTransaction implements Transaction interface for Redis Streams
type RedisStreamsTransaction struct {
	*BaseTransaction
	driver    *RedisStreamsDriver
	client    *redis.Client
	txPipe    redis.Pipeliner
	commands  []redis.Cmder
}

// NewRedisStreamsTransaction creates a new Redis Streams transaction
func NewRedisStreamsTransaction(driver *RedisStreamsDriver, options *TransactionOptions) (*RedisStreamsTransaction, error) {
	base := NewBaseTransaction(options)
	
	return &RedisStreamsTransaction{
		BaseTransaction: base,
		driver:          driver,
		commands:        make([]redis.Cmder, 0),
	}, nil
}

// Begin starts the Redis Streams transaction
func (rst *RedisStreamsTransaction) Begin(ctx context.Context) error {
	rst.mu.Lock()
	defer rst.mu.Unlock()
	
	if rst.state != TransactionIdle {
		return NewTransactionError(rst.id, rst.state, "transaction not in idle state", nil)
	}
	
	// Use the Redis client from the driver
	rst.client = rst.driver.client
	
	// Start Redis transaction pipeline
	rst.txPipe = rst.client.TxPipeline()
	
	rst.setState(TransactionActive)
	rst.startTime = rst.startTime
	
	// Setup timeout if configured
	if rst.options.Timeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, rst.options.Timeout)
		rst.timeoutCancel = cancel
		
		go rst.handleTimeout(timeoutCtx)
	}
	
	return nil
}

// handleTimeout handles transaction timeout for Redis Streams
func (rst *RedisStreamsTransaction) handleTimeout(ctx context.Context) {
	<-ctx.Done()
	
	if ctx.Err() == context.DeadlineExceeded && rst.options.AutoRollback {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*rst.options.Timeout)
		defer cancel()
		
		rst.Rollback(rollbackCtx)
	}
}

// Send sends a message within the Redis Streams transaction
func (rst *RedisStreamsTransaction) Send(ctx context.Context, topic string, message *Message) error {
	rst.mu.RLock()
	defer rst.mu.RUnlock()
	
	if rst.state != TransactionActive {
		return NewTransactionError(rst.id, rst.state, "transaction not active", nil)
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
	
	// Add to transaction pipeline
	cmd := rst.txPipe.XAdd(ctx, &redis.XAddArgs{
		Stream: topic,
		ID:     "*", // Let Redis generate ID
		Values: fields,
	})
	
	rst.commands = append(rst.commands, cmd)
	
	// Set TTL if specified
	if message.TTL > 0 {
		ttlCmd := rst.txPipe.Expire(ctx, topic, message.TTL)
		rst.commands = append(rst.commands, ttlCmd)
	}
	
	// Record the operation
	rst.addOperation(TransactionOperation{
		Type:    "send",
		Topic:   topic,
		Message: message,
	})
	
	return nil
}

// SendBatch sends multiple messages within the Redis Streams transaction
func (rst *RedisStreamsTransaction) SendBatch(ctx context.Context, topic string, messages []*Message) error {
	for _, message := range messages {
		if err := rst.Send(ctx, topic, message); err != nil {
			return err
		}
	}
	
	// Record batch operation
	rst.addOperation(TransactionOperation{
		Type:     "send_batch",
		Topic:    topic,
		Messages: messages,
		Metadata: map[string]interface{}{
			"batch_size": len(messages),
		},
	})
	
	return nil
}

// Prepare prepares the Redis Streams transaction for commit
func (rst *RedisStreamsTransaction) Prepare(ctx context.Context) error {
	rst.mu.Lock()
	defer rst.mu.Unlock()
	
	if rst.state != TransactionActive {
		return NewTransactionError(rst.id, rst.state, "transaction not active", nil)
	}
	
	if !rst.options.TwoPhaseCommit {
		return NewTransactionError(rst.id, rst.state, "two-phase commit not enabled", nil)
	}
	
	rst.setState(TransactionPrepared)
	return nil
}

// Commit commits the Redis Streams transaction
func (rst *RedisStreamsTransaction) Commit(ctx context.Context) error {
	rst.mu.Lock()
	defer rst.mu.Unlock()
	
	if rst.state != TransactionActive && rst.state != TransactionPrepared {
		return NewTransactionError(rst.id, rst.state, "transaction not in committable state", nil)
	}
	
	if rst.txPipe == nil {
		return NewTransactionError(rst.id, rst.state, "transaction pipeline not initialized", nil)
	}
	
	// Execute the transaction pipeline
	results, err := rst.txPipe.Exec(ctx)
	if err != nil {
		rst.setState(TransactionFailed)
		return NewTransactionError(rst.id, rst.state, "failed to execute transaction pipeline", err)
	}
	
	// Check if all commands succeeded
	for i, result := range results {
		if result.Err() != nil {
			rst.setState(TransactionFailed)
			return NewTransactionError(rst.id, rst.state, 
				fmt.Sprintf("command %d failed in transaction", i), result.Err())
		}
	}
	
	rst.setState(TransactionCommitted)
	
	// Record commit operation
	rst.addOperation(TransactionOperation{
		Type: "commit",
		Metadata: map[string]interface{}{
			"command_count":   len(rst.commands),
			"operation_count": len(rst.operations),
			"duration_ms":     rst.Duration().Milliseconds(),
		},
	})
	
	return nil
}

// Rollback rolls back the Redis Streams transaction
func (rst *RedisStreamsTransaction) Rollback(ctx context.Context) error {
	rst.mu.Lock()
	defer rst.mu.Unlock()
	
	if rst.state != TransactionActive && rst.state != TransactionPrepared {
		return NewTransactionError(rst.id, rst.state, "transaction not in rollback-able state", nil)
	}
	
	// Discard the pipeline
	if rst.txPipe != nil {
		rst.txPipe.Discard()
	}
	
	rst.setState(TransactionRolledBack)
	
	// Record rollback operation
	rst.addOperation(TransactionOperation{
		Type: "rollback",
		Metadata: map[string]interface{}{
			"command_count":   len(rst.commands),
			"operation_count": len(rst.operations),
			"duration_ms":     rst.Duration().Milliseconds(),
		},
	})
	
	return nil
}

// Close closes the Redis Streams transaction and releases resources
func (rst *RedisStreamsTransaction) Close() error {
	rst.mu.Lock()
	defer rst.mu.Unlock()
	
	// Cancel timeout if active
	if rst.timeoutCancel != nil {
		rst.timeoutCancel()
		rst.timeoutCancel = nil
	}
	
	// Clean up pipeline
	if rst.txPipe != nil {
		rst.txPipe = nil
	}
	
	// Clear commands
	rst.commands = rst.commands[:0]
	
	return nil
}