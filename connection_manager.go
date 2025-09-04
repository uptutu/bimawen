package bimawen

import (
	"context"
	"fmt"
	"time"
)

// GenericConnectionPool is a generic interface for connection pools
type GenericConnectionPool interface {
	// HealthCheck checks if the pool is healthy
	HealthCheck(ctx context.Context) error
	
	// Stats returns pool statistics
	Stats() map[string]interface{}
	
	// Close closes the pool and all connections
	Close() error
}

// ConnectionPoolManager manages different types of connection pools
type ConnectionPoolManager struct {
	pools map[string]GenericConnectionPool
}

// NewConnectionPoolManager creates a new connection pool manager
func NewConnectionPoolManager() *ConnectionPoolManager {
	return &ConnectionPoolManager{
		pools: make(map[string]GenericConnectionPool),
	}
}

// RegisterPool registers a connection pool with a name
func (m *ConnectionPoolManager) RegisterPool(name string, pool GenericConnectionPool) {
	m.pools[name] = pool
}

// GetPool returns a connection pool by name
func (m *ConnectionPoolManager) GetPool(name string) GenericConnectionPool {
	return m.pools[name]
}

// HealthCheckAll performs health check on all registered pools
func (m *ConnectionPoolManager) HealthCheckAll(ctx context.Context) map[string]error {
	results := make(map[string]error)
	
	for name, pool := range m.pools {
		results[name] = pool.HealthCheck(ctx)
	}
	
	return results
}

// StatsAll returns statistics for all registered pools
func (m *ConnectionPoolManager) StatsAll() map[string]map[string]interface{} {
	results := make(map[string]map[string]interface{})
	
	for name, pool := range m.pools {
		results[name] = pool.Stats()
	}
	
	return results
}

// CloseAll closes all registered pools
func (m *ConnectionPoolManager) CloseAll() map[string]error {
	results := make(map[string]error)
	
	for name, pool := range m.pools {
		results[name] = pool.Close()
	}
	
	return results
}

// AutoReconnectConfig contains configuration for auto-reconnect functionality
type AutoReconnectConfig struct {
	// Enabled enables auto-reconnect functionality
	Enabled bool
	
	// MaxRetries is the maximum number of reconnect attempts
	MaxRetries int
	
	// InitialDelay is the initial delay before first reconnect attempt
	InitialDelay time.Duration
	
	// MaxDelay is the maximum delay between reconnect attempts
	MaxDelay time.Duration
	
	// BackoffMultiplier is the multiplier for exponential backoff
	BackoffMultiplier float64
	
	// HealthCheckInterval is how often to check connection health
	HealthCheckInterval time.Duration
	
	// OnReconnect is called when a reconnection occurs
	OnReconnect func(attempt int, err error)
	
	// OnReconnectSuccess is called when reconnection succeeds
	OnReconnectSuccess func(attempt int)
	
	// OnReconnectFailed is called when all reconnect attempts fail
	OnReconnectFailed func(finalError error)
}

// DefaultAutoReconnectConfig returns default auto-reconnect configuration
func DefaultAutoReconnectConfig() *AutoReconnectConfig {
	return &AutoReconnectConfig{
		Enabled:             true,
		MaxRetries:          5,
		InitialDelay:        1 * time.Second,
		MaxDelay:            60 * time.Second,
		BackoffMultiplier:   2.0,
		HealthCheckInterval: 30 * time.Second,
	}
}

// ConnectionHealthChecker provides health checking functionality
type ConnectionHealthChecker struct {
	config   *AutoReconnectConfig
	callback func() error // Function to test connection health
	stopCh   chan struct{}
}

// NewConnectionHealthChecker creates a new connection health checker
func NewConnectionHealthChecker(config *AutoReconnectConfig, healthCheckFunc func() error) *ConnectionHealthChecker {
	if config == nil {
		config = DefaultAutoReconnectConfig()
	}
	
	return &ConnectionHealthChecker{
		config:   config,
		callback: healthCheckFunc,
		stopCh:   make(chan struct{}),
	}
}

// Start starts the health checker
func (h *ConnectionHealthChecker) Start() {
	if !h.config.Enabled {
		return
	}
	
	go h.healthCheckLoop()
}

// Stop stops the health checker
func (h *ConnectionHealthChecker) Stop() {
	close(h.stopCh)
}

// healthCheckLoop performs periodic health checks
func (h *ConnectionHealthChecker) healthCheckLoop() {
	ticker := time.NewTicker(h.config.HealthCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			err := h.callback()
			if err != nil {
				// Connection unhealthy, trigger reconnect
				go h.attemptReconnect()
			}
		}
	}
}

// attemptReconnect attempts to reconnect with exponential backoff
func (h *ConnectionHealthChecker) attemptReconnect() {
	delay := h.config.InitialDelay
	
	for attempt := 1; attempt <= h.config.MaxRetries; attempt++ {
		select {
		case <-h.stopCh:
			return
		default:
		}
		
		// Wait before attempting reconnect
		time.Sleep(delay)
		
		// Attempt reconnect
		err := h.callback()
		
		// Call reconnect callback
		if h.config.OnReconnect != nil {
			h.config.OnReconnect(attempt, err)
		}
		
		if err == nil {
			// Reconnection successful
			if h.config.OnReconnectSuccess != nil {
				h.config.OnReconnectSuccess(attempt)
			}
			return
		}
		
		// Increase delay for next attempt
		delay = time.Duration(float64(delay) * h.config.BackoffMultiplier)
		if delay > h.config.MaxDelay {
			delay = h.config.MaxDelay
		}
	}
	
	// All reconnect attempts failed
	if h.config.OnReconnectFailed != nil {
		h.config.OnReconnectFailed(fmt.Errorf("failed to reconnect after %d attempts", h.config.MaxRetries))
	}
}

// ConnectionStats provides standardized connection statistics
type ConnectionStats struct {
	// Total number of connections
	TotalConnections int `json:"total_connections"`
	
	// Number of active/healthy connections
	ActiveConnections int `json:"active_connections"`
	
	// Number of failed connections
	FailedConnections int `json:"failed_connections"`
	
	// Pool type (rabbitmq, redis, etc.)
	PoolType string `json:"pool_type"`
	
	// Last health check time
	LastHealthCheck time.Time `json:"last_health_check"`
	
	// Additional pool-specific stats
	Details map[string]interface{} `json:"details,omitempty"`
}

// PooledConnectionInterface defines the interface for pooled connections
type PooledConnectionInterface interface {
	// IsHealthy returns true if the connection is healthy
	IsHealthy() bool
	
	// LastUsed returns the last time this connection was used
	LastUsed() time.Time
	
	// FailureCount returns the number of recent failures
	FailureCount() int
	
	// Close closes the connection
	Close() error
	
	// ID returns a unique identifier for this connection
	ID() string
}