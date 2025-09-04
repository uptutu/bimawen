package bimawen

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConnectionPool manages a pool of Redis connections with auto-reconnect
type RedisConnectionPool struct {
	options          *redis.Options
	maxConnections   int
	connections      []*PooledRedisConnection
	mu               sync.RWMutex
	closed           bool
	reconnectDelay   time.Duration
	healthCheckDelay time.Duration
	idleTimeout      time.Duration
}

// PooledRedisConnection wraps a Redis client with additional metadata
type PooledRedisConnection struct {
	client       *redis.Client
	mu           sync.RWMutex
	closed       bool
	lastUsed     time.Time
	connectionID string
	failureCount int
}

// RedisPoolConfig contains configuration for Redis connection pool
type RedisPoolConfig struct {
	MaxConnections   int
	ReconnectDelay   time.Duration
	HealthCheckDelay time.Duration
	IdleTimeout      time.Duration
	ConnectTimeout   time.Duration
}

// DefaultRedisPoolConfig returns default Redis connection pool configuration
func DefaultRedisPoolConfig() *RedisPoolConfig {
	return &RedisPoolConfig{
		MaxConnections:   10,
		ReconnectDelay:   5 * time.Second,
		HealthCheckDelay: 30 * time.Second,
		IdleTimeout:      10 * time.Minute,
		ConnectTimeout:   5 * time.Second,
	}
}

// NewRedisConnectionPool creates a new Redis connection pool
func NewRedisConnectionPool(options *redis.Options, config *RedisPoolConfig) (*RedisConnectionPool, error) {
	if config == nil {
		config = DefaultRedisPoolConfig()
	}

	pool := &RedisConnectionPool{
		options:          options,
		maxConnections:   config.MaxConnections,
		connections:      make([]*PooledRedisConnection, 0, config.MaxConnections),
		reconnectDelay:   config.ReconnectDelay,
		healthCheckDelay: config.HealthCheckDelay,
		idleTimeout:      config.IdleTimeout,
	}

	// Initialize connections
	for i := 0; i < config.MaxConnections; i++ {
		conn, err := pool.createConnection(i)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to create initial Redis connection %d: %w", i, err)
		}
		pool.connections = append(pool.connections, conn)
	}

	// Start health monitor
	go pool.healthMonitor()

	return pool, nil
}

// createConnection creates a new pooled Redis connection
func (p *RedisConnectionPool) createConnection(id int) (*PooledRedisConnection, error) {
	client := redis.NewClient(p.options)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := client.Ping(ctx).Err()
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to ping Redis server: %w", err)
	}

	pooledConn := &PooledRedisConnection{
		client:       client,
		lastUsed:     time.Now(),
		connectionID: fmt.Sprintf("redis-conn-%d", id),
		failureCount: 0,
	}

	return pooledConn, nil
}

// GetConnection gets a Redis connection from the pool
func (p *RedisConnectionPool) GetConnection() (*redis.Client, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, fmt.Errorf("Redis connection pool is closed")
	}

	// Find the least loaded connection
	var bestConn *PooledRedisConnection
	minFailures := int(^uint(0) >> 1) // Max int

	for _, conn := range p.connections {
		conn.mu.RLock()
		if !conn.closed && conn.failureCount <= minFailures {
			// Check if connection is still alive
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			err := conn.client.Ping(ctx).Err()
			cancel()

			if err == nil {
				minFailures = conn.failureCount
				bestConn = conn
			} else {
				// Connection is dead, mark for reconnection
				conn.failureCount++
			}
		}
		conn.mu.RUnlock()
	}

	if bestConn == nil {
		return nil, fmt.Errorf("no available Redis connections")
	}

	bestConn.mu.Lock()
	bestConn.lastUsed = time.Now()
	bestConn.mu.Unlock()

	return bestConn.client, nil
}

// ReturnConnection returns a connection to the pool (Redis doesn't need explicit return)
func (p *RedisConnectionPool) ReturnConnection(client *redis.Client) {
	// For Redis, we just update the last used time
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, conn := range p.connections {
		if conn.client == client {
			conn.mu.Lock()
			conn.lastUsed = time.Now()
			conn.mu.Unlock()
			break
		}
	}
}

// reconnectConnection attempts to reconnect a failed connection
func (p *RedisConnectionPool) reconnectConnection(oldConn *PooledRedisConnection) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	// Find the connection in the pool and replace it
	for i, conn := range p.connections {
		if conn == oldConn {
			// Retry connection with exponential backoff
			var newConn *PooledRedisConnection
			var err error

			backoff := p.reconnectDelay
			for attempts := 0; attempts < 5 && !p.closed; attempts++ {
				time.Sleep(backoff)
				newConn, err = p.createConnection(i)
				if err == nil {
					break
				}
				backoff *= 2
				if backoff > 60*time.Second {
					backoff = 60 * time.Second
				}
			}

			if err == nil {
				// Close old connection
				oldConn.mu.Lock()
				oldConn.closed = true
				oldConn.client.Close()
				oldConn.mu.Unlock()

				// Replace with new connection
				p.connections[i] = newConn
			}
			break
		}
	}
}

// healthMonitor monitors connection health and performs cleanup
func (p *RedisConnectionPool) healthMonitor() {
	ticker := time.NewTicker(p.healthCheckDelay)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.performHealthCheck()
		}

		p.mu.RLock()
		if p.closed {
			p.mu.RUnlock()
			return
		}
		p.mu.RUnlock()
	}
}

// performHealthCheck checks connection health and triggers reconnection if needed
func (p *RedisConnectionPool) performHealthCheck() {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return
	}

	now := time.Now()
	for _, conn := range p.connections {
		conn.mu.Lock()
		if !conn.closed {
			// Check if connection is still alive
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := conn.client.Ping(ctx).Err()
			cancel()

			if err != nil {
				conn.failureCount++
				if conn.failureCount > 3 {
					// Connection consistently failing, trigger reconnection
					conn.mu.Unlock()
					go p.reconnectConnection(conn)
					continue
				}
			} else {
				// Reset failure count on successful ping
				conn.failureCount = 0
			}

			// Check for idle connections
			idle := now.Sub(conn.lastUsed)
			if idle > p.idleTimeout && conn.failureCount == 0 {
				// Optionally close idle connections (Redis handles this well)
				// For now, just log or reset stats
				conn.lastUsed = now // Reset to avoid constant checks
			}
		}
		conn.mu.Unlock()
	}
}

// HealthCheck checks if the pool is healthy
func (p *RedisConnectionPool) HealthCheck(ctx context.Context) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return fmt.Errorf("Redis connection pool is closed")
	}

	activeConnections := 0
	for _, conn := range p.connections {
		conn.mu.RLock()
		if !conn.closed {
			// Quick health check
			pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			err := conn.client.Ping(pingCtx).Err()
			cancel()

			if err == nil {
				activeConnections++
			}
		}
		conn.mu.RUnlock()
	}

	if activeConnections == 0 {
		return fmt.Errorf("no active Redis connections available")
	}

	return nil
}

// Close closes the Redis connection pool and all connections
func (p *RedisConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true

	// Close all connections
	for _, conn := range p.connections {
		conn.mu.Lock()
		conn.closed = true
		if conn.client != nil {
			conn.client.Close()
		}
		conn.mu.Unlock()
	}

	return nil
}

// Stats returns pool statistics
func (p *RedisConnectionPool) Stats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := map[string]interface{}{
		"max_connections": p.maxConnections,
		"connections":     make([]map[string]interface{}, 0, len(p.connections)),
	}

	activeConnections := 0
	totalFailures := 0

	for i, conn := range p.connections {
		conn.mu.RLock()
		connStats := map[string]interface{}{
			"id":            conn.connectionID,
			"index":         i,
			"closed":        conn.closed,
			"failure_count": conn.failureCount,
			"last_used":     conn.lastUsed,
		}

		if !conn.closed {
			// Test connection
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			err := conn.client.Ping(ctx).Err()
			cancel()

			connStats["healthy"] = (err == nil)
			if err == nil {
				activeConnections++
			}
		}

		totalFailures += conn.failureCount
		stats["connections"] = append(stats["connections"].([]map[string]interface{}), connStats)
		conn.mu.RUnlock()
	}

	stats["active_connections"] = activeConnections
	stats["total_failures"] = totalFailures

	return stats
}

// GetPoolStats returns simplified pool statistics
func (p *RedisConnectionPool) GetPoolStats() (int, int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return 0, 0, fmt.Errorf("pool is closed")
	}

	total := len(p.connections)
	active := 0

	for _, conn := range p.connections {
		conn.mu.RLock()
		if !conn.closed && conn.failureCount < 3 {
			active++
		}
		conn.mu.RUnlock()
	}

	return active, total, nil
}