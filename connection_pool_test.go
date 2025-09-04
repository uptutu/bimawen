package bimawen

import (
	"context"
	"fmt"
	"testing"
	"time"
	
	"github.com/redis/go-redis/v9"
)

func TestRedisConnectionPool(t *testing.T) {
	// Skip test if Redis not available
	options := &redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1,
	}
	
	// Test connection first
	testClient := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := testClient.Ping(ctx).Err()
	cancel()
	testClient.Close()
	
	if err != nil {
		t.Skipf("Redis not available for testing: %v", err)
	}
	
	// Create pool
	config := DefaultRedisPoolConfig()
	config.MaxConnections = 3
	config.HealthCheckDelay = 1 * time.Second
	
	pool, err := NewRedisConnectionPool(options, config)
	if err != nil {
		t.Fatalf("Failed to create Redis pool: %v", err)
	}
	defer pool.Close()
	
	// Test getting connections
	conn1, err := pool.GetConnection()
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	
	conn2, err := pool.GetConnection()
	if err != nil {
		t.Fatalf("Failed to get second connection: %v", err)
	}
	
	// Test connections work
	err = conn1.Ping(ctx).Err()
	if err != nil {
		t.Errorf("Connection 1 ping failed: %v", err)
	}
	
	err = conn2.Ping(ctx).Err()
	if err != nil {
		t.Errorf("Connection 2 ping failed: %v", err)
	}
	
	// Return connections
	pool.ReturnConnection(conn1)
	pool.ReturnConnection(conn2)
	
	// Test health check
	err = pool.HealthCheck(context.Background())
	if err != nil {
		t.Errorf("Pool health check failed: %v", err)
	}
	
	// Test statistics
	stats := pool.Stats()
	if stats["max_connections"] != 3 {
		t.Errorf("Expected max_connections 3, got %v", stats["max_connections"])
	}
	
	if stats["active_connections"].(int) == 0 {
		t.Error("Expected some active connections")
	}
}

func TestConnectionPoolManager(t *testing.T) {
	manager := NewConnectionPoolManager()
	
	// Create mock pools for testing
	mockPool1 := &MockConnectionPool{name: "pool1"}
	mockPool2 := &MockConnectionPool{name: "pool2"}
	
	// Register pools
	manager.RegisterPool("test-pool-1", mockPool1)
	manager.RegisterPool("test-pool-2", mockPool2)
	
	// Test retrieving pool
	retrieved := manager.GetPool("test-pool-1")
	if retrieved != mockPool1 {
		t.Error("Failed to retrieve correct pool")
	}
	
	// Test health check all
	results := manager.HealthCheckAll(context.Background())
	if len(results) != 2 {
		t.Errorf("Expected 2 health check results, got %d", len(results))
	}
	
	// Test stats all
	statsResults := manager.StatsAll()
	if len(statsResults) != 2 {
		t.Errorf("Expected 2 stats results, got %d", len(statsResults))
	}
	
	// Test close all
	closeResults := manager.CloseAll()
	if len(closeResults) != 2 {
		t.Errorf("Expected 2 close results, got %d", len(closeResults))
	}
}

func TestAutoReconnectConfig(t *testing.T) {
	config := DefaultAutoReconnectConfig()
	
	// Test defaults
	if !config.Enabled {
		t.Error("Expected auto-reconnect to be enabled by default")
	}
	if config.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries 5, got %d", config.MaxRetries)
	}
	if config.InitialDelay != 1*time.Second {
		t.Errorf("Expected InitialDelay 1s, got %v", config.InitialDelay)
	}
	if config.MaxDelay != 60*time.Second {
		t.Errorf("Expected MaxDelay 60s, got %v", config.MaxDelay)
	}
	if config.BackoffMultiplier != 2.0 {
		t.Errorf("Expected BackoffMultiplier 2.0, got %f", config.BackoffMultiplier)
	}
}

func TestConnectionHealthChecker(t *testing.T) {
	// Test configuration
	config := DefaultAutoReconnectConfig()
	config.HealthCheckInterval = 100 * time.Millisecond
	config.MaxRetries = 3
	config.InitialDelay = 50 * time.Millisecond
	
	// Track callbacks
	reconnectCalls := 0
	successCalls := 0
	failedCalls := 0
	
	config.OnReconnect = func(attempt int, err error) {
		reconnectCalls++
	}
	config.OnReconnectSuccess = func(attempt int) {
		successCalls++
	}
	config.OnReconnectFailed = func(finalError error) {
		failedCalls++
	}
	
	// Health check function that fails initially, then succeeds
	attempts := 0
	healthCheckFunc := func() error {
		attempts++
		if attempts <= 2 {
			return fmt.Errorf("connection failed (attempt %d)", attempts)
		}
		return nil // Success after 2 attempts
	}
	
	// Create health checker
	checker := NewConnectionHealthChecker(config, healthCheckFunc)
	
	// Start health checking
	checker.Start()
	
	// Wait for health checks and reconnection attempts
	time.Sleep(300 * time.Millisecond)
	
	// Stop checker
	checker.Stop()
	
	// Give it a moment to stop
	time.Sleep(50 * time.Millisecond)
	
	// Verify callbacks were called
	if reconnectCalls == 0 {
		t.Error("Expected some reconnect attempts")
	}
	
	// Since health check eventually succeeds, we should see success
	if attempts < 2 {
		t.Errorf("Expected at least 2 health check attempts, got %d", attempts)
	}
}

func TestConnectionPoolWithCircuitBreaker(t *testing.T) {
	// This test demonstrates integration with circuit breaker
	cbConfig := DefaultCircuitBreakerConfig()
	cbConfig.Enabled = true
	cbConfig.FailureThreshold = 2
	cbConfig.Timeout = 100 * time.Millisecond
	
	cb := NewCircuitBreaker(cbConfig)
	
	// Simulate pool operations with circuit breaker
	attempts := 0
	poolOperation := func(ctx context.Context) error {
		attempts++
		if attempts <= 3 {
			return fmt.Errorf("pool operation failed (attempt %d)", attempts)
		}
		return nil
	}
	
	ctx := context.Background()
	
	// First few calls should fail and open circuit
	for i := 0; i < 5; i++ {
		err := cb.Call(ctx, poolOperation)
		if err != nil && IsCircuitBreakerError(err) {
			t.Logf("Circuit breaker opened after %d operations", i+1)
			break
		}
	}
	
	// Verify circuit breaker state
	if cb.State() != StateOpen {
		t.Errorf("Expected circuit breaker to be open, got %s", cb.State())
	}
	
	// Wait for circuit to allow retry
	time.Sleep(150 * time.Millisecond)
	
	// Try again - should transition to half-open and potentially close
	err := cb.Call(ctx, poolOperation)
	if err != nil {
		t.Logf("Operation still failing: %v", err)
	} else {
		t.Log("Operation succeeded after circuit breaker recovery")
	}
}

func TestRedisPoolStats(t *testing.T) {
	options := &redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1,
	}
	
	// Test Redis availability
	testClient := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := testClient.Ping(ctx).Err()
	cancel()
	testClient.Close()
	
	if err != nil {
		t.Skipf("Redis not available for testing: %v", err)
	}
	
	config := DefaultRedisPoolConfig()
	config.MaxConnections = 2
	
	pool, err := NewRedisConnectionPool(options, config)
	if err != nil {
		t.Fatalf("Failed to create Redis pool: %v", err)
	}
	defer pool.Close()
	
	// Get detailed stats
	stats := pool.Stats()
	
	// Verify expected fields
	expectedFields := []string{"max_connections", "connections", "active_connections", "total_failures"}
	for _, field := range expectedFields {
		if _, exists := stats[field]; !exists {
			t.Errorf("Missing expected field in stats: %s", field)
		}
	}
	
	// Verify connections array
	connections, ok := stats["connections"].([]map[string]interface{})
	if !ok {
		t.Error("Expected connections to be an array of maps")
	}
	
	if len(connections) != 2 {
		t.Errorf("Expected 2 connection stats, got %d", len(connections))
	}
	
	// Test simplified stats
	active, total, err := pool.GetPoolStats()
	if err != nil {
		t.Errorf("GetPoolStats failed: %v", err)
	}
	
	if total != 2 {
		t.Errorf("Expected total connections 2, got %d", total)
	}
	
	if active == 0 {
		t.Error("Expected some active connections")
	}
	
	t.Logf("Pool stats: %d active out of %d total connections", active, total)
}

// MockConnectionPool is a mock implementation for testing
type MockConnectionPool struct {
	name   string
	closed bool
}

func (m *MockConnectionPool) HealthCheck(ctx context.Context) error {
	if m.closed {
		return fmt.Errorf("pool %s is closed", m.name)
	}
	return nil
}

func (m *MockConnectionPool) Stats() map[string]interface{} {
	return map[string]interface{}{
		"name":   m.name,
		"closed": m.closed,
	}
}

func (m *MockConnectionPool) Close() error {
	m.closed = true
	return nil
}

// Benchmark tests
func BenchmarkRedisPoolGetConnection(b *testing.B) {
	options := &redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1,
	}
	
	// Check if Redis is available
	testClient := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := testClient.Ping(ctx).Err()
	cancel()
	testClient.Close()
	
	if err != nil {
		b.Skipf("Redis not available for benchmarking: %v", err)
	}
	
	config := DefaultRedisPoolConfig()
	pool, err := NewRedisConnectionPool(options, config)
	if err != nil {
		b.Fatalf("Failed to create Redis pool: %v", err)
	}
	defer pool.Close()
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		conn, err := pool.GetConnection()
		if err != nil {
			b.Fatalf("Failed to get connection: %v", err)
		}
		pool.ReturnConnection(conn)
	}
}

func BenchmarkRedisPoolHealthCheck(b *testing.B) {
	options := &redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1,
	}
	
	testClient := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := testClient.Ping(ctx).Err()
	cancel()
	testClient.Close()
	
	if err != nil {
		b.Skipf("Redis not available for benchmarking: %v", err)
	}
	
	config := DefaultRedisPoolConfig()
	pool, err := NewRedisConnectionPool(options, config)
	if err != nil {
		b.Fatalf("Failed to create Redis pool: %v", err)
	}
	defer pool.Close()
	
	ctx = context.Background()
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		err := pool.HealthCheck(ctx)
		if err != nil {
			b.Fatalf("Health check failed: %v", err)
		}
	}
}