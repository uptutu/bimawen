package bimawen

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TransactionState represents the state of a transaction
type TransactionState int

const (
	// TransactionIdle transaction is idle/not started
	TransactionIdle TransactionState = iota
	// TransactionActive transaction is active and accepting operations
	TransactionActive
	// TransactionPrepared transaction is prepared for commit (two-phase commit)
	TransactionPrepared
	// TransactionCommitted transaction has been committed
	TransactionCommitted
	// TransactionRolledBack transaction has been rolled back
	TransactionRolledBack
	// TransactionFailed transaction has failed
	TransactionFailed
)

// String returns string representation of transaction state
func (s TransactionState) String() string {
	switch s {
	case TransactionIdle:
		return "idle"
	case TransactionActive:
		return "active"
	case TransactionPrepared:
		return "prepared"
	case TransactionCommitted:
		return "committed"
	case TransactionRolledBack:
		return "rolledback"
	case TransactionFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Transaction defines the interface for message queue transactions
type Transaction interface {
	// Begin starts the transaction
	Begin(ctx context.Context) error
	
	// Send sends a message within the transaction
	Send(ctx context.Context, topic string, message *Message) error
	
	// SendBatch sends multiple messages within the transaction
	SendBatch(ctx context.Context, topic string, messages []*Message) error
	
	// Prepare prepares the transaction for commit (two-phase commit)
	Prepare(ctx context.Context) error
	
	// Commit commits the transaction
	Commit(ctx context.Context) error
	
	// Rollback rolls back the transaction
	Rollback(ctx context.Context) error
	
	// State returns the current transaction state
	State() TransactionState
	
	// ID returns the unique transaction ID
	ID() string
	
	// Close closes the transaction and releases resources
	Close() error
}

// TransactionOptions contains configuration for transactions
type TransactionOptions struct {
	// ID is the transaction ID (auto-generated if empty)
	ID string
	
	// Timeout is the transaction timeout
	Timeout time.Duration
	
	// Isolation level for the transaction
	IsolationLevel TransactionIsolationLevel
	
	// TwoPhaseCommit enables two-phase commit protocol
	TwoPhaseCommit bool
	
	// AutoRollback automatically rollback on context timeout/cancellation
	AutoRollback bool
	
	// OnStateChange callback when transaction state changes
	OnStateChange func(oldState, newState TransactionState, txnID string)
	
	// OnError callback when transaction encounters an error
	OnError func(err error, txnID string)
}

// TransactionIsolationLevel defines transaction isolation levels
type TransactionIsolationLevel int

const (
	// IsolationReadUncommitted allows reading uncommitted changes
	IsolationReadUncommitted TransactionIsolationLevel = iota
	// IsolationReadCommitted only allows reading committed changes
	IsolationReadCommitted
	// IsolationRepeatableRead prevents dirty reads and non-repeatable reads
	IsolationRepeatableRead
	// IsolationSerializable highest isolation level
	IsolationSerializable
)

// String returns string representation of isolation level
func (l TransactionIsolationLevel) String() string {
	switch l {
	case IsolationReadUncommitted:
		return "read_uncommitted"
	case IsolationReadCommitted:
		return "read_committed"
	case IsolationRepeatableRead:
		return "repeatable_read"
	case IsolationSerializable:
		return "serializable"
	default:
		return "unknown"
	}
}

// DefaultTransactionOptions returns default transaction options
func DefaultTransactionOptions() *TransactionOptions {
	return &TransactionOptions{
		Timeout:        30 * time.Second,
		IsolationLevel: IsolationReadCommitted,
		TwoPhaseCommit: false,
		AutoRollback:   true,
	}
}

// TransactionManager manages transactions across different drivers
type TransactionManager struct {
	transactions map[string]Transaction
	mu           sync.RWMutex
	driver       Driver
	options      *TransactionOptions
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(driver Driver, options *TransactionOptions) *TransactionManager {
	if options == nil {
		options = DefaultTransactionOptions()
	}
	
	return &TransactionManager{
		transactions: make(map[string]Transaction),
		driver:       driver,
		options:      options,
	}
}

// BeginTransaction starts a new transaction
func (tm *TransactionManager) BeginTransaction(ctx context.Context, options *TransactionOptions) (Transaction, error) {
	if options == nil {
		options = tm.options
	}
	
	// Generate ID if not provided
	if options.ID == "" {
		options.ID = generateTransactionID()
	}
	
	// Check if transaction already exists
	tm.mu.RLock()
	if _, exists := tm.transactions[options.ID]; exists {
		tm.mu.RUnlock()
		return nil, fmt.Errorf("transaction with ID %s already exists", options.ID)
	}
	tm.mu.RUnlock()
	
	// Create transaction based on driver type
	var txn Transaction
	var err error
	
	switch driver := tm.driver.(type) {
	case *RabbitMQDriver:
		txn, err = NewRabbitMQTransaction(driver, options)
	case *RedisDriver:
		txn, err = NewRedisTransaction(driver, options)
	case *RedisStreamsDriver:
		txn, err = NewRedisStreamsTransaction(driver, options)
	default:
		// Check if driver implements transaction interface directly
		if transactionalDriver, ok := tm.driver.(interface {
			NewTransaction(options *TransactionOptions) (Transaction, error)
		}); ok {
			txn, err = transactionalDriver.NewTransaction(options)
		} else {
			return nil, fmt.Errorf("transaction not supported for driver type: %T", driver)
		}
	}
	
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction: %w", err)
	}
	
	// Start the transaction
	err = txn.Begin(ctx)
	if err != nil {
		txn.Close()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	
	// Register transaction
	tm.mu.Lock()
	tm.transactions[options.ID] = txn
	tm.mu.Unlock()
	
	return txn, nil
}

// GetTransaction returns a transaction by ID
func (tm *TransactionManager) GetTransaction(id string) (Transaction, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	txn, exists := tm.transactions[id]
	return txn, exists
}

// CommitTransaction commits a transaction by ID
func (tm *TransactionManager) CommitTransaction(ctx context.Context, id string) error {
	txn, exists := tm.GetTransaction(id)
	if !exists {
		return fmt.Errorf("transaction %s not found", id)
	}
	
	err := txn.Commit(ctx)
	
	// Remove from active transactions
	tm.mu.Lock()
	delete(tm.transactions, id)
	tm.mu.Unlock()
	
	txn.Close()
	return err
}

// RollbackTransaction rolls back a transaction by ID
func (tm *TransactionManager) RollbackTransaction(ctx context.Context, id string) error {
	txn, exists := tm.GetTransaction(id)
	if !exists {
		return fmt.Errorf("transaction %s not found", id)
	}
	
	err := txn.Rollback(ctx)
	
	// Remove from active transactions
	tm.mu.Lock()
	delete(tm.transactions, id)
	tm.mu.Unlock()
	
	txn.Close()
	return err
}

// ListTransactions returns all active transaction IDs
func (tm *TransactionManager) ListTransactions() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	
	ids := make([]string, 0, len(tm.transactions))
	for id := range tm.transactions {
		ids = append(ids, id)
	}
	return ids
}

// GetTransactionStats returns statistics for all transactions
func (tm *TransactionManager) GetTransactionStats() map[string]TransactionState {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	
	stats := make(map[string]TransactionState)
	for id, txn := range tm.transactions {
		stats[id] = txn.State()
	}
	return stats
}

// Close closes all transactions and the manager
func (tm *TransactionManager) Close() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	
	var lastErr error
	for id, txn := range tm.transactions {
		// Try to rollback active transactions
		if txn.State() == TransactionActive {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			txn.Rollback(ctx)
			cancel()
		}
		
		if err := txn.Close(); err != nil {
			lastErr = err
		}
		delete(tm.transactions, id)
	}
	
	return lastErr
}

// BaseTransaction provides common transaction functionality
type BaseTransaction struct {
	id             string
	state          TransactionState
	options        *TransactionOptions
	mu             sync.RWMutex
	startTime      time.Time
	operations     []TransactionOperation
	timeoutCancel  context.CancelFunc
}

// TransactionOperation represents an operation within a transaction
type TransactionOperation struct {
	Type      string                 `json:"type"`
	Topic     string                 `json:"topic"`
	Message   *Message               `json:"message,omitempty"`
	Messages  []*Message             `json:"messages,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// NewBaseTransaction creates a new base transaction
func NewBaseTransaction(options *TransactionOptions) *BaseTransaction {
	if options == nil {
		options = DefaultTransactionOptions()
	}
	
	if options.ID == "" {
		options.ID = generateTransactionID()
	}
	
	return &BaseTransaction{
		id:         options.ID,
		state:      TransactionIdle,
		options:    options,
		operations: make([]TransactionOperation, 0),
	}
}

// ID returns the transaction ID
func (bt *BaseTransaction) ID() string {
	return bt.id
}

// State returns the current transaction state
func (bt *BaseTransaction) State() TransactionState {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.state
}

// setState changes the transaction state and triggers callbacks
func (bt *BaseTransaction) setState(newState TransactionState) {
	bt.mu.Lock()
	oldState := bt.state
	bt.state = newState
	bt.mu.Unlock()
	
	// Trigger callback if configured
	if bt.options.OnStateChange != nil {
		bt.options.OnStateChange(oldState, newState, bt.id)
	}
}

// addOperation records an operation in the transaction
func (bt *BaseTransaction) addOperation(op TransactionOperation) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	
	op.Timestamp = time.Now()
	bt.operations = append(bt.operations, op)
}

// GetOperations returns all operations in the transaction
func (bt *BaseTransaction) GetOperations() []TransactionOperation {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	ops := make([]TransactionOperation, len(bt.operations))
	copy(ops, bt.operations)
	return ops
}

// IsActive returns true if the transaction is active
func (bt *BaseTransaction) IsActive() bool {
	return bt.State() == TransactionActive
}

// IsFinished returns true if the transaction is finished (committed/rolled back)
func (bt *BaseTransaction) IsFinished() bool {
	state := bt.State()
	return state == TransactionCommitted || state == TransactionRolledBack || state == TransactionFailed
}

// Duration returns how long the transaction has been active
func (bt *BaseTransaction) Duration() time.Duration {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	
	if bt.startTime.IsZero() {
		return 0
	}
	return time.Since(bt.startTime)
}

// generateTransactionID generates a unique transaction ID
func generateTransactionID() string {
	return fmt.Sprintf("txn_%d_%d", time.Now().UnixNano(), time.Now().Nanosecond())
}

// TransactionError represents a transaction-specific error
type TransactionError struct {
	TxnID   string
	State   TransactionState
	Message string
	Cause   error
}

// Error implements the error interface
func (e *TransactionError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("transaction %s (state: %s): %s: %v", e.TxnID, e.State, e.Message, e.Cause)
	}
	return fmt.Sprintf("transaction %s (state: %s): %s", e.TxnID, e.State, e.Message)
}

// Unwrap implements error unwrapping for Go 1.13+
func (e *TransactionError) Unwrap() error {
	return e.Cause
}

// NewTransactionError creates a new transaction error
func NewTransactionError(txnID string, state TransactionState, message string, cause error) *TransactionError {
	return &TransactionError{
		TxnID:   txnID,
		State:   state,
		Message: message,
		Cause:   cause,
	}
}

// IsTransactionError checks if an error is a transaction error
func IsTransactionError(err error) bool {
	_, ok := err.(*TransactionError)
	return ok
}