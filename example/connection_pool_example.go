// Connection Pool and Auto-Reconnect Example
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	bimawen "local.git/libs/bimawen.git"
)

func main() {
	fmt.Println("=== Connection Pool and Auto-Reconnect Example ===")

	// Example 1: Redis Connection Pool
	fmt.Println("\n1. Redis Connection Pool Example")
	testRedisConnectionPool()

	// Example 2: Connection Pool Manager
	fmt.Println("\n2. Connection Pool Manager Example")
	testConnectionPoolManager()

	// Example 3: Auto-Reconnect Configuration
	fmt.Println("\n3. Auto-Reconnect with Health Monitoring")
	testAutoReconnectHealth()

	// Example 4: Stress test with multiple connections
	fmt.Println("\n4. Connection Pool Stress Test")
	stressTestConnectionPool()

	fmt.Println("\n=== Connection Pool Example Complete ===")
}

func testRedisConnectionPool() {
	// Redis connection options
	options := &redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}

	// Connection pool configuration
	config := bimawen.DefaultRedisPoolConfig()
	config.MaxConnections = 5
	config.HealthCheckDelay = 10 * time.Second
	config.ReconnectDelay = 2 * time.Second
	config.IdleTimeout = 30 * time.Second

	// Create Redis connection pool
	pool, err := bimawen.NewRedisConnectionPool(options, config)
	if err != nil {
		fmt.Printf("❌ Failed to create Redis connection pool: %v\n", err)
		return
	}
	defer pool.Close()

	fmt.Println("✅ Redis connection pool created successfully")

	// Test getting and using connections
	for i := 0; i < 3; i++ {
		conn, err := pool.GetConnection()
		if err != nil {
			fmt.Printf("❌ Failed to get connection %d: %v\n", i, err)
			continue
		}

		// Use the connection
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = conn.Set(ctx, fmt.Sprintf("pool-test-%d", i), fmt.Sprintf("value-%d", i), time.Minute).Err()
		cancel()

		if err != nil {
			fmt.Printf("❌ Failed to set value with connection %d: %v\n", i, err)
		} else {
			fmt.Printf("✅ Successfully used connection %d\n", i)
		}

		// Return connection to pool
		pool.ReturnConnection(conn)
	}

	// Health check
	ctx := context.Background()
	err = pool.HealthCheck(ctx)
	if err != nil {
		fmt.Printf("❌ Pool health check failed: %v\n", err)
	} else {
		fmt.Println("✅ Pool health check passed")
	}

	// Display pool statistics
	stats := pool.Stats()
	fmt.Printf("📊 Pool Stats: Max Connections: %d, Active: %d\n",
		stats["max_connections"], stats["active_connections"])
}

func testConnectionPoolManager() {
	manager := bimawen.NewConnectionPoolManager()

	// Create multiple mock pools for demonstration
	mockPools := []struct {
		name string
		pool *MockPool
	}{
		{"redis-main", &MockPool{name: "redis-main", healthy: true}},
		{"redis-cache", &MockPool{name: "redis-cache", healthy: true}},
		{"rabbitmq-main", &MockPool{name: "rabbitmq-main", healthy: false}},
	}

	// Register pools
	for _, mp := range mockPools {
		manager.RegisterPool(mp.name, mp.pool)
		fmt.Printf("✅ Registered pool: %s\n", mp.name)
	}

	// Perform health checks on all pools
	fmt.Println("\n🏥 Performing health checks on all pools:")
	healthResults := manager.HealthCheckAll(context.Background())
	for name, err := range healthResults {
		if err != nil {
			fmt.Printf("❌ %s: %v\n", name, err)
		} else {
			fmt.Printf("✅ %s: healthy\n", name)
		}
	}

	// Get statistics for all pools
	fmt.Println("\n📊 Pool Statistics:")
	allStats := manager.StatsAll()
	for name, stats := range allStats {
		fmt.Printf("📊 %s: %v\n", name, stats)
	}

	// Close all pools
	fmt.Println("\n🔒 Closing all pools:")
	closeResults := manager.CloseAll()
	for name, err := range closeResults {
		if err != nil {
			fmt.Printf("❌ Failed to close %s: %v\n", name, err)
		} else {
			fmt.Printf("✅ Closed %s successfully\n", name)
		}
	}
}

func testAutoReconnectHealth() {
	// Configure auto-reconnect
	config := bimawen.DefaultAutoReconnectConfig()
	config.MaxRetries = 3
	config.InitialDelay = 500 * time.Millisecond
	config.MaxDelay = 5 * time.Second
	config.HealthCheckInterval = 2 * time.Second

	// Track reconnection attempts
	reconnectAttempts := 0
	successfulReconnects := 0
	failedReconnects := 0

	config.OnReconnect = func(attempt int, err error) {
		reconnectAttempts++
		if err != nil {
			fmt.Printf("🔄 Reconnect attempt %d failed: %v\n", attempt, err)
		} else {
			fmt.Printf("🔄 Reconnect attempt %d...\n", attempt)
		}
	}

	config.OnReconnectSuccess = func(attempt int) {
		successfulReconnects++
		fmt.Printf("✅ Reconnection successful after %d attempts\n", attempt)
	}

	config.OnReconnectFailed = func(finalError error) {
		failedReconnects++
		fmt.Printf("❌ All reconnection attempts failed: %v\n", finalError)
	}

	// Simulate a connection that fails initially, then recovers
	healthCheckAttempts := 0
	healthCheckFunc := func() error {
		healthCheckAttempts++
		if healthCheckAttempts <= 2 {
			return fmt.Errorf("connection unavailable (attempt %d)", healthCheckAttempts)
		}
		return nil // Connection recovered
	}

	// Create and start health checker
	checker := bimawen.NewConnectionHealthChecker(config, healthCheckFunc)
	checker.Start()

	fmt.Println("🏥 Starting health monitoring...")
	fmt.Println("💡 Simulating connection failure and recovery...")

	// Let it run for a while
	time.Sleep(8 * time.Second)

	// Stop the health checker
	checker.Stop()

	// Summary
	fmt.Printf("\n📈 Auto-Reconnect Summary:\n")
	fmt.Printf("  - Health check attempts: %d\n", healthCheckAttempts)
	fmt.Printf("  - Reconnect attempts: %d\n", reconnectAttempts)
	fmt.Printf("  - Successful reconnects: %d\n", successfulReconnects)
	fmt.Printf("  - Failed reconnects: %d\n", failedReconnects)
}

func stressTestConnectionPool() {
	fmt.Println("🔥 Running connection pool stress test...")

	// Create a mock pool for stress testing
	options := &redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1,
	}

	// Test if Redis is available for stress test
	testClient := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := testClient.Ping(ctx).Err()
	cancel()
	testClient.Close()

	if err != nil {
		fmt.Printf("⚠️  Redis not available for stress test, using simulation\n")
		simulateStressTest()
		return
	}

	// Real Redis stress test
	config := bimawen.DefaultRedisPoolConfig()
	config.MaxConnections = 10
	config.HealthCheckDelay = 1 * time.Second

	pool, err := bimawen.NewRedisConnectionPool(options, config)
	if err != nil {
		fmt.Printf("❌ Failed to create pool for stress test: %v\n", err)
		return
	}
	defer pool.Close()

	// Run concurrent operations
	numWorkers := 50
	operationsPerWorker := 20
	var wg sync.WaitGroup
	successCount := 0
	errorCount := 0
	var mu sync.Mutex

	startTime := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < operationsPerWorker; j++ {
				conn, err := pool.GetConnection()
				if err != nil {
					mu.Lock()
					errorCount++
					mu.Unlock()
					continue
				}

				// Perform operation
				key := fmt.Sprintf("stress-test-%d-%d", workerID, j)
				value := fmt.Sprintf("worker-%d-op-%d", workerID, j)

				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				err = conn.Set(ctx, key, value, time.Minute).Err()
				cancel()

				pool.ReturnConnection(conn)

				mu.Lock()
				if err != nil {
					errorCount++
				} else {
					successCount++
				}
				mu.Unlock()

				// Small delay to simulate real workload
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	// Wait for all workers to complete
	wg.Wait()
	duration := time.Since(startTime)

	// Final stats
	totalOps := numWorkers * operationsPerWorker
	opsPerSecond := float64(totalOps) / duration.Seconds()

	fmt.Printf("✅ Stress test completed in %v\n", duration)
	fmt.Printf("📊 Results:\n")
	fmt.Printf("  - Total operations: %d\n", totalOps)
	fmt.Printf("  - Successful: %d\n", successCount)
	fmt.Printf("  - Errors: %d\n", errorCount)
	fmt.Printf("  - Success rate: %.1f%%\n", float64(successCount)/float64(totalOps)*100)
	fmt.Printf("  - Operations/second: %.1f\n", opsPerSecond)

	// Final pool stats
	active, total, err := pool.GetPoolStats()
	if err == nil {
		fmt.Printf("  - Final pool state: %d active / %d total connections\n", active, total)
	}
}

func simulateStressTest() {
	fmt.Println("🎭 Running simulated stress test...")

	numOperations := 1000
	successCount := 0

	start := time.Now()
	for i := 0; i < numOperations; i++ {
		// Simulate getting connection from pool
		time.Sleep(100 * time.Microsecond)

		// Simulate operation success (95% success rate)
		if i%20 != 0 {
			successCount++
		}
	}
	duration := time.Since(start)

	fmt.Printf("✅ Simulated stress test completed in %v\n", duration)
	fmt.Printf("📊 Simulated Results:\n")
	fmt.Printf("  - Operations: %d\n", numOperations)
	fmt.Printf("  - Success rate: %.1f%%\n", float64(successCount)/float64(numOperations)*100)
	fmt.Printf("  - Operations/second: %.1f\n", float64(numOperations)/duration.Seconds())
}

// MockPool implements GenericConnectionPool for demonstration
type MockPool struct {
	name    string
	healthy bool
	closed  bool
}

func (m *MockPool) HealthCheck(ctx context.Context) error {
	if m.closed {
		return fmt.Errorf("pool %s is closed", m.name)
	}
	if !m.healthy {
		return fmt.Errorf("pool %s is unhealthy", m.name)
	}
	return nil
}

func (m *MockPool) Stats() map[string]interface{} {
	return map[string]interface{}{
		"name":    m.name,
		"healthy": m.healthy,
		"closed":  m.closed,
		"type":    "mock",
	}
}

func (m *MockPool) Close() error {
	m.closed = true
	return nil
}
