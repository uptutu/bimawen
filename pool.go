package bimawen

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConnectionPool manages a pool of RabbitMQ connections with TLS support
type ConnectionPool struct {
	uri              string
	maxConnections   int
	maxChannels      int
	connections      []*PooledConnection
	channels         chan *amqp.Channel
	mu               sync.RWMutex
	closed           bool
	reconnectDelay   time.Duration
	heartbeatTimeout time.Duration
	tlsConfig        *TLSConfig
}

// PooledConnection wraps an AMQP connection with additional metadata
type PooledConnection struct {
	conn       *amqp.Connection
	channels   chan *amqp.Channel
	mu         sync.RWMutex
	closed     bool
	lastUsed   time.Time
	channelCount int
	maxChannels int
}

// ConnectionPoolConfig contains configuration for connection pool
type ConnectionPoolConfig struct {
	MaxConnections   int
	MaxChannels      int
	ReconnectDelay   time.Duration
	HeartbeatTimeout time.Duration
	IdleTimeout      time.Duration
	TLSConfig        *TLSConfig
}

// DefaultConnectionPoolConfig returns default connection pool configuration
func DefaultConnectionPoolConfig() *ConnectionPoolConfig {
	return &ConnectionPoolConfig{
		MaxConnections:   5,
		MaxChannels:      100,
		ReconnectDelay:   5 * time.Second,
		HeartbeatTimeout: 60 * time.Second,
		IdleTimeout:      10 * time.Minute,
	}
}

// NewConnectionPool creates a new connection pool with TLS support
func NewConnectionPool(uri string, config *ConnectionPoolConfig) (*ConnectionPool, error) {
	if config == nil {
		config = DefaultConnectionPoolConfig()
	}

	pool := &ConnectionPool{
		uri:              uri,
		maxConnections:   config.MaxConnections,
		maxChannels:      config.MaxChannels,
		connections:      make([]*PooledConnection, 0, config.MaxConnections),
		channels:         make(chan *amqp.Channel, config.MaxChannels),
		reconnectDelay:   config.ReconnectDelay,
		heartbeatTimeout: config.HeartbeatTimeout,
		tlsConfig:        config.TLSConfig,
	}

	// Initialize connections
	for i := 0; i < config.MaxConnections; i++ {
		conn, err := pool.createConnection()
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to create initial connection %d: %w", i, err)
		}
		pool.connections = append(pool.connections, conn)
	}

	// Start connection health monitor
	go pool.healthMonitor()

	return pool, nil
}

// createConnection creates a new pooled connection with TLS support
func (p *ConnectionPool) createConnection() (*PooledConnection, error) {
	var conn *amqp.Connection
	var err error

	// Use TLS if configured
	if p.tlsConfig != nil && p.tlsConfig.Enabled {
		tlsConfig, err := p.tlsConfig.BuildTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		conn, err = amqp.DialTLS(p.uri, tlsConfig)
	} else {
		conn, err = amqp.Dial(p.uri)
	}
	
	if err != nil {
		return nil, err
	}

	pooledConn := &PooledConnection{
		conn:        conn,
		channels:    make(chan *amqp.Channel, p.maxChannels/p.maxConnections),
		lastUsed:    time.Now(),
		maxChannels: p.maxChannels / p.maxConnections,
	}

	// Pre-create some channels
	for i := 0; i < 10; i++ {
		ch, err := conn.Channel()
		if err != nil {
			break
		}
		pooledConn.channels <- ch
		pooledConn.channelCount++
	}

	// Listen for connection close events
	go pooledConn.handleConnectionClose(p)

	return pooledConn, nil
}

// handleConnectionClose handles connection close events and triggers reconnection
func (pc *PooledConnection) handleConnectionClose(pool *ConnectionPool) {
	closeErr := <-pc.conn.NotifyClose(make(chan *amqp.Error))
	if closeErr != nil {
		pc.mu.Lock()
		pc.closed = true
		// Close all channels in the pool
		close(pc.channels)
		for ch := range pc.channels {
			ch.Close()
		}
		pc.mu.Unlock()

		// Trigger reconnection
		pool.reconnectConnection(pc)
	}
}

// reconnectConnection attempts to reconnect a failed connection
func (p *ConnectionPool) reconnectConnection(oldConn *PooledConnection) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	// Find the connection in the pool and replace it
	for i, conn := range p.connections {
		if conn == oldConn {
			// Retry connection with exponential backoff
			var newConn *PooledConnection
			var err error
			
			backoff := p.reconnectDelay
			for attempts := 0; attempts < 5 && !p.closed; attempts++ {
				time.Sleep(backoff)
				newConn, err = p.createConnection()
				if err == nil {
					break
				}
				backoff *= 2
				if backoff > 60*time.Second {
					backoff = 60 * time.Second
				}
			}

			if err == nil {
				p.connections[i] = newConn
			}
			break
		}
	}
}

// GetChannel gets a channel from the pool
func (p *ConnectionPool) GetChannel() (*amqp.Channel, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, fmt.Errorf("connection pool is closed")
	}

	// Try to get a channel from any connection
	for _, conn := range p.connections {
		if ch := conn.getChannel(); ch != nil {
			return ch, nil
		}
	}

	// If no cached channel available, create a new one
	return p.createNewChannel()
}

// createNewChannel creates a new channel from the least loaded connection
func (p *ConnectionPool) createNewChannel() (*amqp.Channel, error) {
	var bestConn *PooledConnection
	minChannels := int(^uint(0) >> 1) // Max int

	for _, conn := range p.connections {
		conn.mu.RLock()
		if !conn.closed && conn.channelCount < minChannels {
			minChannels = conn.channelCount
			bestConn = conn
		}
		conn.mu.RUnlock()
	}

	if bestConn == nil {
		return nil, fmt.Errorf("no available connections")
	}

	return bestConn.createChannel()
}

// getChannel gets a channel from this connection's pool
func (pc *PooledConnection) getChannel() *amqp.Channel {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if pc.closed {
		return nil
	}

	select {
	case ch := <-pc.channels:
		pc.lastUsed = time.Now()
		return ch
	default:
		return nil
	}
}

// createChannel creates a new channel from this connection
func (pc *PooledConnection) createChannel() (*amqp.Channel, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.closed || pc.channelCount >= pc.maxChannels {
		return nil, fmt.Errorf("connection closed or max channels reached")
	}

	ch, err := pc.conn.Channel()
	if err != nil {
		return nil, err
	}

	pc.channelCount++
	pc.lastUsed = time.Now()
	return ch, nil
}

// ReturnChannel returns a channel to the pool
func (p *ConnectionPool) ReturnChannel(ch *amqp.Channel) {
	if ch == nil {
		return
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		ch.Close()
		return
	}

	// Find which connection this channel belongs to and return it
	for _, conn := range p.connections {
		if conn.returnChannel(ch) {
			break
		}
	}
}

// returnChannel returns a channel to this connection's pool
func (pc *PooledConnection) returnChannel(ch *amqp.Channel) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.closed {
		ch.Close()
		return true
	}

	select {
	case pc.channels <- ch:
		return true
	default:
		// Channel pool is full, close the channel
		ch.Close()
		pc.channelCount--
		return true
	}
}

// GetConnection returns a connection from the pool
func (p *ConnectionPool) GetConnection() (*amqp.Connection, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	if p.closed {
		return nil, fmt.Errorf("connection pool is closed")
	}
	
	// Find a healthy connection
	for _, conn := range p.connections {
		conn.mu.RLock()
		if !conn.closed && !conn.conn.IsClosed() {
			conn.lastUsed = time.Now()
			conn.mu.RUnlock()
			return conn.conn, nil
		}
		conn.mu.RUnlock()
	}
	
	// If no healthy connections found, create a new one
	newConn, err := p.createConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to create new connection: %w", err)
	}
	
	// Add to connections pool
	p.connections = append(p.connections, newConn)
	
	return newConn.conn, nil
}

// ReturnConnection returns a connection to the pool (no-op for connections)
func (p *ConnectionPool) ReturnConnection(conn *amqp.Connection) {
	// For connections, we don't need to do anything special
	// The connection remains in the pool and can be reused
	// This method exists for API consistency with channel management
}

// healthMonitor monitors connection health and performs cleanup
func (p *ConnectionPool) healthMonitor() {
	ticker := time.NewTicker(30 * time.Second)
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

// performHealthCheck checks connection health and cleans up idle resources
func (p *ConnectionPool) performHealthCheck() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	now := time.Now()
	for _, conn := range p.connections {
		conn.mu.Lock()
		if !conn.closed {
			// Check if connection is still alive
			if conn.conn.IsClosed() {
				conn.closed = true
			}
			
			// Clean up idle channels
			idle := now.Sub(conn.lastUsed)
			if idle > 10*time.Minute {
				// Close some idle channels
				for i := 0; i < 5 && conn.channelCount > 10; i++ {
					select {
					case ch := <-conn.channels:
						ch.Close()
						conn.channelCount--
					default:
						break
					}
				}
			}
		}
		conn.mu.Unlock()
	}
}

// HealthCheck checks if the pool is healthy
func (p *ConnectionPool) HealthCheck(ctx context.Context) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return fmt.Errorf("connection pool is closed")
	}

	activeConnections := 0
	for _, conn := range p.connections {
		conn.mu.RLock()
		if !conn.closed && !conn.conn.IsClosed() {
			activeConnections++
		}
		conn.mu.RUnlock()
	}

	if activeConnections == 0 {
		return fmt.Errorf("no active connections available")
	}

	// Try to create a test channel
	ch, err := p.GetChannel()
	if err != nil {
		return fmt.Errorf("failed to get test channel: %w", err)
	}
	p.ReturnChannel(ch)

	return nil
}

// Close closes the connection pool and all connections
func (p *ConnectionPool) Close() error {
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
		close(conn.channels)
		for ch := range conn.channels {
			ch.Close()
		}
		if conn.conn != nil && !conn.conn.IsClosed() {
			conn.conn.Close()
		}
		conn.mu.Unlock()
	}

	close(p.channels)
	return nil
}

// Stats returns pool statistics
func (p *ConnectionPool) Stats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := map[string]interface{}{
		"max_connections": p.maxConnections,
		"max_channels":    p.maxChannels,
		"connections":     make([]map[string]interface{}, 0, len(p.connections)),
	}

	activeConnections := 0
	totalChannels := 0

	for i, conn := range p.connections {
		conn.mu.RLock()
		connStats := map[string]interface{}{
			"index":         i,
			"closed":        conn.closed,
			"is_conn_closed": conn.conn.IsClosed(),
			"channel_count": conn.channelCount,
			"last_used":     conn.lastUsed,
		}
		
		if !conn.closed && !conn.conn.IsClosed() {
			activeConnections++
		}
		totalChannels += conn.channelCount
		
		stats["connections"] = append(stats["connections"].([]map[string]interface{}), connStats)
		conn.mu.RUnlock()
	}

	stats["active_connections"] = activeConnections
	stats["total_channels"] = totalChannels

	return stats
}