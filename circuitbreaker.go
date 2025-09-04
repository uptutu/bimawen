package bimawen

import (
	"context"
	"sync"
	"time"
)

// CircuitBreakerState represents the state of the circuit breaker
type CircuitBreakerState int

const (
	// StateClosed circuit breaker is closed, requests are allowed
	StateClosed CircuitBreakerState = iota
	// StateOpen circuit breaker is open, requests are denied
	StateOpen
	// StateHalfOpen circuit breaker is half-open, testing if service recovered
	StateHalfOpen
)

// String returns string representation of the state
func (s CircuitBreakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerConfig contains circuit breaker configuration
type CircuitBreakerConfig struct {
	// Enabled enables circuit breaker functionality
	Enabled bool
	
	// FailureThreshold number of failures before opening the circuit
	FailureThreshold int
	
	// SuccessThreshold number of successes in half-open state before closing
	SuccessThreshold int
	
	// Timeout duration the circuit stays open before moving to half-open
	Timeout time.Duration
	
	// MaxRequests maximum number of requests allowed when half-open
	MaxRequests int
	
	// ResetTimeout timeout for resetting failure count when closed
	ResetTimeout time.Duration
	
	// OnStateChange callback when state changes
	OnStateChange func(from, to CircuitBreakerState)
}

// DefaultCircuitBreakerConfig returns default circuit breaker configuration
func DefaultCircuitBreakerConfig() *CircuitBreakerConfig {
	return &CircuitBreakerConfig{
		Enabled:          false,
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          60 * time.Second,
		MaxRequests:      10,
		ResetTimeout:     60 * time.Second,
		OnStateChange:    nil,
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config           *CircuitBreakerConfig
	state            CircuitBreakerState
	failureCount     int
	successCount     int
	requestCount     int
	lastFailureTime  time.Time
	lastSuccessTime  time.Time
	nextRetry        time.Time
	mu               sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	if config == nil {
		config = DefaultCircuitBreakerConfig()
	}
	
	return &CircuitBreaker{
		config: config,
		state:  StateClosed,
	}
}

// Call executes a function with circuit breaker protection
func (cb *CircuitBreaker) Call(ctx context.Context, fn func(ctx context.Context) error) error {
	if !cb.config.Enabled {
		return fn(ctx) // Circuit breaker disabled, execute directly
	}
	
	// Check if we can execute the request
	if err := cb.beforeRequest(); err != nil {
		return err
	}
	
	// Execute the function
	err := fn(ctx)
	
	// Record the result
	cb.afterRequest(err)
	
	return err
}

// beforeRequest checks if a request can be executed
func (cb *CircuitBreaker) beforeRequest() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	switch cb.state {
	case StateClosed:
		return nil // Allow request
		
	case StateOpen:
		if time.Now().After(cb.nextRetry) {
			cb.setState(StateHalfOpen)
			// Don't increment request count here - wait for afterRequest
			return nil // Allow request in half-open state
		}
		return NewCircuitBreakerError("circuit breaker is open")
		
	case StateHalfOpen:
		if cb.requestCount >= cb.config.MaxRequests {
			return NewCircuitBreakerError("circuit breaker half-open: max requests exceeded")
		}
		// Don't increment here - increment after the call succeeds/fails
		return nil // Allow request
		
	default:
		return NewCircuitBreakerError("unknown circuit breaker state")
	}
}

// afterRequest records the result of a request
func (cb *CircuitBreaker) afterRequest(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	// Increment request count for half-open state
	if cb.state == StateHalfOpen {
		cb.requestCount++
	}
	
	if err != nil {
		cb.onFailure()
	} else {
		cb.onSuccess()
	}
}

// onFailure handles a failed request
func (cb *CircuitBreaker) onFailure() {
	cb.failureCount++
	cb.lastFailureTime = time.Now()
	
	switch cb.state {
	case StateClosed:
		if cb.failureCount >= cb.config.FailureThreshold {
			cb.setState(StateOpen)
			cb.nextRetry = time.Now().Add(cb.config.Timeout)
		}
		
	case StateHalfOpen:
		cb.setState(StateOpen)
		cb.nextRetry = time.Now().Add(cb.config.Timeout)
	}
}

// onSuccess handles a successful request
func (cb *CircuitBreaker) onSuccess() {
	cb.lastSuccessTime = time.Now()
	
	switch cb.state {
	case StateClosed:
		// Reset failure count after a period of success
		if time.Since(cb.lastFailureTime) > cb.config.ResetTimeout {
			cb.failureCount = 0
		}
		
	case StateHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.config.SuccessThreshold {
			cb.setState(StateClosed)
			cb.failureCount = 0
			cb.successCount = 0
			cb.requestCount = 0
		}
	}
}

// setState changes the circuit breaker state
func (cb *CircuitBreaker) setState(state CircuitBreakerState) {
	if cb.state == state {
		return
	}
	
	prevState := cb.state
	cb.state = state
	
	// Reset counters when changing state
	if state == StateOpen {
		cb.successCount = 0
		cb.requestCount = 0
	} else if state == StateClosed {
		cb.failureCount = 0
		cb.successCount = 0
		cb.requestCount = 0
	} else if state == StateHalfOpen {
		cb.successCount = 0
		cb.requestCount = 0
	}
	
	// Trigger callback if configured
	if cb.config.OnStateChange != nil {
		cb.config.OnStateChange(prevState, state)
	}
}

// State returns the current state
func (cb *CircuitBreaker) State() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Stats returns circuit breaker statistics
func (cb *CircuitBreaker) Stats() CircuitBreakerStats {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	return CircuitBreakerStats{
		State:           cb.state,
		FailureCount:    cb.failureCount,
		SuccessCount:    cb.successCount,
		RequestCount:    cb.requestCount,
		LastFailureTime: cb.lastFailureTime,
		LastSuccessTime: cb.lastSuccessTime,
		NextRetry:       cb.nextRetry,
	}
}

// Reset resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	prevState := cb.state
	cb.state = StateClosed
	cb.failureCount = 0
	cb.successCount = 0
	cb.requestCount = 0
	cb.lastFailureTime = time.Time{}
	cb.lastSuccessTime = time.Time{}
	cb.nextRetry = time.Time{}
	
	if cb.config.OnStateChange != nil && prevState != StateClosed {
		cb.config.OnStateChange(prevState, StateClosed)
	}
}

// CircuitBreakerStats contains circuit breaker statistics
type CircuitBreakerStats struct {
	State           CircuitBreakerState
	FailureCount    int
	SuccessCount    int
	RequestCount    int
	LastFailureTime time.Time
	LastSuccessTime time.Time
	NextRetry       time.Time
}

// CircuitBreakerError represents a circuit breaker error
type CircuitBreakerError struct {
	message string
}

// NewCircuitBreakerError creates a new circuit breaker error
func NewCircuitBreakerError(message string) *CircuitBreakerError {
	return &CircuitBreakerError{message: message}
}

// Error implements the error interface
func (e *CircuitBreakerError) Error() string {
	return e.message
}

// IsCircuitBreakerError checks if an error is a circuit breaker error
func IsCircuitBreakerError(err error) bool {
	_, ok := err.(*CircuitBreakerError)
	return ok
}