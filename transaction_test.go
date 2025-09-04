package bimawen

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestTransactionStateTransitions(t *testing.T) {
	// Test state string representations
	states := []struct {
		state    TransactionState
		expected string
	}{
		{TransactionIdle, "idle"},
		{TransactionActive, "active"},
		{TransactionPrepared, "prepared"},
		{TransactionCommitted, "committed"},
		{TransactionRolledBack, "rolledback"},
		{TransactionFailed, "failed"},
	}
	
	for _, test := range states {
		if test.state.String() != test.expected {
			t.Errorf("Expected state %v to be %s, got %s", test.state, test.expected, test.state.String())
		}
	}
}

func TestTransactionIsolationLevels(t *testing.T) {
	levels := []struct {
		level    TransactionIsolationLevel
		expected string
	}{
		{IsolationReadUncommitted, "read_uncommitted"},
		{IsolationReadCommitted, "read_committed"},
		{IsolationRepeatableRead, "repeatable_read"},
		{IsolationSerializable, "serializable"},
	}
	
	for _, test := range levels {
		if test.level.String() != test.expected {
			t.Errorf("Expected isolation level %v to be %s, got %s", test.level, test.expected, test.level.String())
		}
	}
}

func TestDefaultTransactionOptions(t *testing.T) {
	options := DefaultTransactionOptions()
	
	if options.Timeout != 30*time.Second {
		t.Errorf("Expected default timeout 30s, got %v", options.Timeout)
	}
	if options.IsolationLevel != IsolationReadCommitted {
		t.Errorf("Expected default isolation level ReadCommitted, got %v", options.IsolationLevel)
	}
	if options.TwoPhaseCommit != false {
		t.Error("Expected default two-phase commit to be false")
	}
	if options.AutoRollback != true {
		t.Error("Expected default auto-rollback to be true")
	}
}

func TestBaseTransaction(t *testing.T) {
	options := &TransactionOptions{
		ID:      "test-txn-123",
		Timeout: 10 * time.Second,
	}
	
	base := NewBaseTransaction(options)
	
	// Test initial state
	if base.ID() != "test-txn-123" {
		t.Errorf("Expected ID 'test-txn-123', got '%s'", base.ID())
	}
	if base.State() != TransactionIdle {
		t.Errorf("Expected initial state Idle, got %v", base.State())
	}
	if base.IsActive() {
		t.Error("Expected transaction to not be active initially")
	}
	if base.IsFinished() {
		t.Error("Expected transaction to not be finished initially")
	}
	
	// Test state changes
	stateChanges := []TransactionState{}
	options.OnStateChange = func(old, new TransactionState, txnID string) {
		stateChanges = append(stateChanges, new)
	}
	
	base.setState(TransactionActive)
	base.setState(TransactionCommitted)
	
	if len(stateChanges) != 2 {
		t.Errorf("Expected 2 state changes, got %d", len(stateChanges))
	}
	if stateChanges[0] != TransactionActive {
		t.Errorf("Expected first state change to Active, got %v", stateChanges[0])
	}
	if stateChanges[1] != TransactionCommitted {
		t.Errorf("Expected second state change to Committed, got %v", stateChanges[1])
	}
	
	// Test operations
	op1 := TransactionOperation{
		Type:  "send",
		Topic: "test-topic",
	}
	base.addOperation(op1)
	
	ops := base.GetOperations()
	if len(ops) != 1 {
		t.Errorf("Expected 1 operation, got %d", len(ops))
	}
	if ops[0].Type != "send" {
		t.Errorf("Expected operation type 'send', got '%s'", ops[0].Type)
	}
	if ops[0].Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", ops[0].Topic)
	}
}

func TestTransactionManager(t *testing.T) {
	// Create mock driver
	mockDriver := &MockTransactionalDriver{}
	
	manager := NewTransactionManager(mockDriver, DefaultTransactionOptions())
	defer manager.Close()
	
	// Test beginning a transaction
	ctx := context.Background()
	txn, err := manager.BeginTransaction(ctx, &TransactionOptions{
		ID:      "test-txn-manager",
		Timeout: 5 * time.Second,
	})
	
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	if txn.ID() != "test-txn-manager" {
		t.Errorf("Expected transaction ID 'test-txn-manager', got '%s'", txn.ID())
	}
	if txn.State() != TransactionActive {
		t.Errorf("Expected transaction state Active, got %v", txn.State())
	}
	
	// Test getting transaction
	retrievedTxn, exists := manager.GetTransaction("test-txn-manager")
	if !exists {
		t.Error("Expected to find transaction 'test-txn-manager'")
	}
	if retrievedTxn != txn {
		t.Error("Retrieved transaction should be the same instance")
	}
	
	// Test listing transactions
	txnList := manager.ListTransactions()
	if len(txnList) != 1 {
		t.Errorf("Expected 1 transaction in list, got %d", len(txnList))
	}
	if txnList[0] != "test-txn-manager" {
		t.Errorf("Expected transaction 'test-txn-manager' in list, got '%s'", txnList[0])
	}
	
	// Test stats
	stats := manager.GetTransactionStats()
	if len(stats) != 1 {
		t.Errorf("Expected 1 transaction in stats, got %d", len(stats))
	}
	if stats["test-txn-manager"] != TransactionActive {
		t.Errorf("Expected transaction state Active in stats, got %v", stats["test-txn-manager"])
	}
	
	// Test committing transaction
	err = manager.CommitTransaction(ctx, "test-txn-manager")
	if err != nil {
		t.Errorf("Failed to commit transaction: %v", err)
	}
	
	// Transaction should be removed from manager after commit
	_, exists = manager.GetTransaction("test-txn-manager")
	if exists {
		t.Error("Transaction should be removed after commit")
	}
}

func TestTransactionManagerDuplicateID(t *testing.T) {
	mockDriver := &MockTransactionalDriver{}
	manager := NewTransactionManager(mockDriver, DefaultTransactionOptions())
	defer manager.Close()
	
	ctx := context.Background()
	
	// Begin first transaction
	_, err := manager.BeginTransaction(ctx, &TransactionOptions{
		ID: "duplicate-test",
	})
	if err != nil {
		t.Fatalf("Failed to begin first transaction: %v", err)
	}
	
	// Try to begin second transaction with same ID
	_, err = manager.BeginTransaction(ctx, &TransactionOptions{
		ID: "duplicate-test",
	})
	if err == nil {
		t.Error("Expected error when creating transaction with duplicate ID")
	}
}

func TestTransactionTimeout(t *testing.T) {
	mockDriver := &MockTransactionalDriver{}
	manager := NewTransactionManager(mockDriver, DefaultTransactionOptions())
	defer manager.Close()
	
	// Create transaction with very short timeout
	ctx := context.Background()
	txn, err := manager.BeginTransaction(ctx, &TransactionOptions{
		ID:           "timeout-test",
		Timeout:      100 * time.Millisecond,
		AutoRollback: true,
	})
	
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	// Wait for timeout
	time.Sleep(200 * time.Millisecond)
	
	// Transaction should be rolled back due to timeout
	state := txn.State()
	if state != TransactionRolledBack && state != TransactionFailed {
		t.Errorf("Expected transaction to be rolled back or failed due to timeout, got %v", state)
	}
}

func TestTransactionError(t *testing.T) {
	originalErr := fmt.Errorf("original error")
	txnErr := NewTransactionError("test-txn", TransactionFailed, "test failed", originalErr)
	
	// Test error message
	expectedMsg := "transaction test-txn (state: failed): test failed: original error"
	if txnErr.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, txnErr.Error())
	}
	
	// Test error unwrapping
	if txnErr.Unwrap() != originalErr {
		t.Error("Expected Unwrap to return original error")
	}
	
	// Test error type checking
	if !IsTransactionError(txnErr) {
		t.Error("Expected IsTransactionError to return true")
	}
	
	regularErr := fmt.Errorf("regular error")
	if IsTransactionError(regularErr) {
		t.Error("Expected IsTransactionError to return false for regular error")
	}
}

func TestTransactionOperations(t *testing.T) {
	base := NewBaseTransaction(&TransactionOptions{
		ID: "ops-test",
	})
	
	// Test adding operations
	message1 := &Message{
		ID:   "msg-1",
		Body: []byte("test message 1"),
	}
	
	message2 := &Message{
		ID:   "msg-2",
		Body: []byte("test message 2"),
	}
	
	// Add send operation
	base.addOperation(TransactionOperation{
		Type:    "send",
		Topic:   "topic-1",
		Message: message1,
	})
	
	// Add batch operation
	base.addOperation(TransactionOperation{
		Type:     "send_batch",
		Topic:    "topic-2",
		Messages: []*Message{message1, message2},
		Metadata: map[string]interface{}{
			"batch_size": 2,
		},
	})
	
	// Add commit operation
	base.addOperation(TransactionOperation{
		Type: "commit",
		Metadata: map[string]interface{}{
			"duration_ms": 150,
		},
	})
	
	ops := base.GetOperations()
	if len(ops) != 3 {
		t.Errorf("Expected 3 operations, got %d", len(ops))
	}
	
	// Verify send operation
	if ops[0].Type != "send" {
		t.Errorf("Expected first operation type 'send', got '%s'", ops[0].Type)
	}
	if ops[0].Topic != "topic-1" {
		t.Errorf("Expected first operation topic 'topic-1', got '%s'", ops[0].Topic)
	}
	if ops[0].Message.ID != "msg-1" {
		t.Errorf("Expected first operation message ID 'msg-1', got '%s'", ops[0].Message.ID)
	}
	
	// Verify batch operation
	if ops[1].Type != "send_batch" {
		t.Errorf("Expected second operation type 'send_batch', got '%s'", ops[1].Type)
	}
	if len(ops[1].Messages) != 2 {
		t.Errorf("Expected 2 messages in batch operation, got %d", len(ops[1].Messages))
	}
	if ops[1].Metadata["batch_size"] != 2 {
		t.Errorf("Expected batch_size metadata to be 2, got %v", ops[1].Metadata["batch_size"])
	}
	
	// Verify commit operation
	if ops[2].Type != "commit" {
		t.Errorf("Expected third operation type 'commit', got '%s'", ops[2].Type)
	}
	if ops[2].Metadata["duration_ms"] != 150 {
		t.Errorf("Expected duration_ms metadata to be 150, got %v", ops[2].Metadata["duration_ms"])
	}
	
	// Verify timestamps are set
	for i, op := range ops {
		if op.Timestamp.IsZero() {
			t.Errorf("Expected operation %d to have timestamp set", i)
		}
	}
}

// MockTransactionalDriver is a mock driver that supports transactions
type MockTransactionalDriver struct {
	transactions map[string]*MockTransaction
}

func (m *MockTransactionalDriver) NewProducer(options *ProducerOptions) (Producer, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockTransactionalDriver) NewConsumer(options *ConsumerOptions) (Consumer, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockTransactionalDriver) HealthCheck(ctx context.Context) error {
	return nil
}

func (m *MockTransactionalDriver) Close() error {
	return nil
}

func (m *MockTransactionalDriver) NewTransaction(options *TransactionOptions) (Transaction, error) {
	if m.transactions == nil {
		m.transactions = make(map[string]*MockTransaction)
	}
	
	txn := &MockTransaction{
		BaseTransaction: NewBaseTransaction(options),
		driver:          m,
	}
	
	m.transactions[options.ID] = txn
	return txn, nil
}

// MockTransaction implements Transaction interface for testing
type MockTransaction struct {
	*BaseTransaction
	driver *MockTransactionalDriver
}

func (mt *MockTransaction) Begin(ctx context.Context) error {
	if mt.state != TransactionIdle {
		return NewTransactionError(mt.id, mt.state, "transaction not in idle state", nil)
	}
	
	mt.setState(TransactionActive)
	mt.startTime = time.Now()
	
	// Setup timeout if configured
	if mt.options.Timeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, mt.options.Timeout)
		mt.timeoutCancel = cancel
		
		go mt.handleTimeout(timeoutCtx)
	}
	
	return nil
}

// handleTimeout handles transaction timeout for MockTransaction
func (mt *MockTransaction) handleTimeout(ctx context.Context) {
	<-ctx.Done()
	
	if ctx.Err() == context.DeadlineExceeded && mt.options.AutoRollback {
		// Auto-rollback on timeout
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*mt.options.Timeout)
		defer cancel()
		
		mt.Rollback(rollbackCtx)
	}
}

func (mt *MockTransaction) Send(ctx context.Context, topic string, message *Message) error {
	if mt.state != TransactionActive {
		return NewTransactionError(mt.id, mt.state, "transaction not active", nil)
	}
	
	mt.addOperation(TransactionOperation{
		Type:    "send",
		Topic:   topic,
		Message: message,
	})
	
	return nil
}

func (mt *MockTransaction) SendBatch(ctx context.Context, topic string, messages []*Message) error {
	if mt.state != TransactionActive {
		return NewTransactionError(mt.id, mt.state, "transaction not active", nil)
	}
	
	mt.addOperation(TransactionOperation{
		Type:     "send_batch",
		Topic:    topic,
		Messages: messages,
	})
	
	return nil
}

func (mt *MockTransaction) Prepare(ctx context.Context) error {
	if mt.state != TransactionActive {
		return NewTransactionError(mt.id, mt.state, "transaction not active", nil)
	}
	
	mt.setState(TransactionPrepared)
	return nil
}

func (mt *MockTransaction) Commit(ctx context.Context) error {
	if mt.state != TransactionActive && mt.state != TransactionPrepared {
		return NewTransactionError(mt.id, mt.state, "transaction not in committable state", nil)
	}
	
	mt.setState(TransactionCommitted)
	return nil
}

func (mt *MockTransaction) Rollback(ctx context.Context) error {
	if mt.state != TransactionActive && mt.state != TransactionPrepared {
		return NewTransactionError(mt.id, mt.state, "transaction not in rollback-able state", nil)
	}
	
	mt.setState(TransactionRolledBack)
	return nil
}

func (mt *MockTransaction) Close() error {
	// Cancel timeout if active
	if mt.timeoutCancel != nil {
		mt.timeoutCancel()
		mt.timeoutCancel = nil
	}
	
	return nil
}

// Test transaction ID generation
func TestTransactionIDGeneration(t *testing.T) {
	// Generate multiple IDs and ensure they're unique
	ids := make(map[string]bool)
	
	for i := 0; i < 100; i++ {
		id := generateTransactionID()
		if ids[id] {
			t.Errorf("Generated duplicate transaction ID: %s", id)
		}
		ids[id] = true
		
		// Verify ID format
		if len(id) == 0 {
			t.Error("Generated empty transaction ID")
		}
		if id[:4] != "txn_" {
			t.Errorf("Expected transaction ID to start with 'txn_', got '%s'", id)
		}
		
		// Small delay to ensure different timestamps
		time.Sleep(time.Nanosecond)
	}
}

// Benchmark transaction operations
func BenchmarkTransactionCreation(b *testing.B) {
	mockDriver := &MockTransactionalDriver{}
	manager := NewTransactionManager(mockDriver, DefaultTransactionOptions())
	defer manager.Close()
	
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn, err := manager.BeginTransaction(ctx, &TransactionOptions{
			ID: fmt.Sprintf("bench-txn-%d", i),
		})
		if err != nil {
			b.Fatalf("Failed to create transaction: %v", err)
		}
		
		// Commit and clean up
		txn.Commit(ctx)
		txn.Close()
	}
}

func BenchmarkTransactionOperations(b *testing.B) {
	base := NewBaseTransaction(&TransactionOptions{ID: "bench-txn"})
	base.setState(TransactionActive)
	
	message := &Message{
		ID:   "bench-msg",
		Body: []byte("benchmark message"),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base.addOperation(TransactionOperation{
			Type:    "send",
			Topic:   "bench-topic",
			Message: message,
		})
	}
}