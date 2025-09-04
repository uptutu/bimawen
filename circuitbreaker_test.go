package bimawen

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCircuitBreakerBasicFunctionality(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.Enabled = true
	config.FailureThreshold = 3
	config.SuccessThreshold = 2
	config.Timeout = 100 * time.Millisecond
	
	cb := NewCircuitBreaker(config)
	
	// Initial state should be closed
	if cb.State() != StateClosed {
		t.Errorf("Expected initial state to be closed, got %s", cb.State())
	}
	
	ctx := context.Background()
	
	// Simulate successful calls
	for i := 0; i < 2; i++ {
		err := cb.Call(ctx, func(ctx context.Context) error {
			return nil // success
		})
		if err != nil {
			t.Errorf("Unexpected error on successful call: %v", err)
		}
	}
	
	// State should still be closed
	if cb.State() != StateClosed {
		t.Errorf("Expected state to remain closed after successes, got %s", cb.State())
	}
	
	// Simulate failures to trigger circuit breaker
	for i := 0; i < 3; i++ {
		err := cb.Call(ctx, func(ctx context.Context) error {
			return fmt.Errorf("simulated failure %d", i)
		})
		if err == nil {
			t.Error("Expected error from failed call")
		}
	}
	
	// State should now be open
	if cb.State() != StateOpen {
		t.Errorf("Expected state to be open after failures, got %s", cb.State())
	}
	
	// Next call should be rejected immediately
	err := cb.Call(ctx, func(ctx context.Context) error {
		return nil
	})
	if err == nil {
		t.Error("Expected circuit breaker error when circuit is open")
	}
	if !IsCircuitBreakerError(err) {
		t.Errorf("Expected circuit breaker error, got %v", err)
	}
}

func TestCircuitBreakerStateTransitions(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.Enabled = true
	config.FailureThreshold = 2
	config.SuccessThreshold = 2
	config.Timeout = 50 * time.Millisecond
	config.MaxRequests = 5
	
	var stateChanges []string
	var mu sync.Mutex
	config.OnStateChange = func(from, to CircuitBreakerState) {
		mu.Lock()
		stateChanges = append(stateChanges, fmt.Sprintf("%s->%s", from, to))
		mu.Unlock()
	}
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	// Trigger failures to open circuit
	for i := 0; i < 2; i++ {
		cb.Call(ctx, func(ctx context.Context) error {
			return fmt.Errorf("failure %d", i)
		})
	}
	
	// Verify circuit is open
	if cb.State() != StateOpen {
		t.Errorf("Expected state open after failures, got %s", cb.State())
	}
	
	// Wait for timeout to allow transition to half-open
	time.Sleep(60 * time.Millisecond)
	
	// Call should transition to half-open and succeed
	err := cb.Call(ctx, func(ctx context.Context) error {
		return nil // success
	})
	if err != nil {
		t.Errorf("Unexpected error in half-open state: %v", err)
	}
	
	// Verify state is half-open
	if cb.State() != StateHalfOpen {
		t.Errorf("Expected state half-open after timeout, got %s", cb.State())
	}
	
	// One more success should close the circuit
	err = cb.Call(ctx, func(ctx context.Context) error {
		return nil // success
	})
	if err != nil {
		t.Errorf("Unexpected error when closing circuit: %v", err)
	}
	
	// Verify circuit is closed
	if cb.State() != StateClosed {
		t.Errorf("Expected state closed after successes, got %s", cb.State())
	}
	
	// Wait a bit for callbacks to complete
	time.Sleep(10 * time.Millisecond)
	
	// Verify state transitions
	mu.Lock()
	defer mu.Unlock()
	
	expectedTransitions := []string{"closed->open", "open->half-open", "half-open->closed"}
	if len(stateChanges) != len(expectedTransitions) {
		t.Errorf("Expected %d state changes, got %d: %v", len(expectedTransitions), len(stateChanges), stateChanges)
		return
	}
	
	for i, expected := range expectedTransitions {
		if stateChanges[i] != expected {
			t.Errorf("Expected state change %d to be %s, got %s", i, expected, stateChanges[i])
		}
	}
}

func TestCircuitBreakerHalfOpenMaxRequests(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.Enabled = true
	config.FailureThreshold = 1
	config.Timeout = 10 * time.Millisecond
	config.MaxRequests = 2
	config.SuccessThreshold = 3 // Higher than MaxRequests to test the limit
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	// Trigger failure to open circuit
	cb.Call(ctx, func(ctx context.Context) error {
		return fmt.Errorf("failure")
	})
	
	if cb.State() != StateOpen {
		t.Errorf("Expected state open after failure, got %s", cb.State())
	}
	
	// Wait for timeout
	time.Sleep(20 * time.Millisecond)
	
	// First request in half-open should succeed
	err := cb.Call(ctx, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Errorf("First request in half-open should succeed: %v", err)
	}
	
	if cb.State() != StateHalfOpen {
		t.Errorf("Expected state half-open after first success, got %s", cb.State())
	}
	
	// Second request in half-open should succeed
	err = cb.Call(ctx, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Errorf("Second request in half-open should succeed: %v", err)
	}
	
	// Third request should be rejected (exceeds MaxRequests)
	err = cb.Call(ctx, func(ctx context.Context) error {
		return nil
	})
	if err == nil {
		t.Error("Third request should be rejected in half-open state")
	}
	if !IsCircuitBreakerError(err) {
		t.Errorf("Expected circuit breaker error, got %v", err)
	}
}

func TestCircuitBreakerStats(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.Enabled = true
	config.FailureThreshold = 2
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	// Initial stats
	stats := cb.Stats()
	if stats.State != StateClosed {
		t.Errorf("Expected initial state closed, got %s", stats.State)
	}
	if stats.FailureCount != 0 {
		t.Errorf("Expected initial failure count 0, got %d", stats.FailureCount)
	}
	
	// Trigger failures
	for i := 0; i < 2; i++ {
		cb.Call(ctx, func(ctx context.Context) error {
			return fmt.Errorf("failure %d", i)
		})
	}
	
	// Check updated stats
	stats = cb.Stats()
	if stats.State != StateOpen {
		t.Errorf("Expected state open after failures, got %s", stats.State)
	}
	if stats.FailureCount != 2 {
		t.Errorf("Expected failure count 2, got %d", stats.FailureCount)
	}
	if stats.LastFailureTime.IsZero() {
		t.Error("Expected last failure time to be set")
	}
}

func TestCircuitBreakerReset(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.Enabled = true
	config.FailureThreshold = 1
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	// Trigger failure to open circuit
	cb.Call(ctx, func(ctx context.Context) error {
		return fmt.Errorf("failure")
	})
	
	if cb.State() != StateOpen {
		t.Errorf("Expected state open after failure, got %s", cb.State())
	}
	
	// Reset circuit breaker
	cb.Reset()
	
	if cb.State() != StateClosed {
		t.Errorf("Expected state closed after reset, got %s", cb.State())
	}
	
	stats := cb.Stats()
	if stats.FailureCount != 0 {
		t.Errorf("Expected failure count 0 after reset, got %d", stats.FailureCount)
	}
}

func TestCircuitBreakerDisabled(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.Enabled = false // disabled
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	// Even with many failures, disabled circuit breaker should allow all calls
	for i := 0; i < 10; i++ {
		err := cb.Call(ctx, func(ctx context.Context) error {
			return fmt.Errorf("failure %d", i)
		})
		// The error should be the original error, not a circuit breaker error
		if err == nil {
			t.Error("Expected error from failed function")
		}
		if IsCircuitBreakerError(err) {
			t.Error("Should not get circuit breaker error when disabled")
		}
	}
}

func TestCircuitBreakerConcurrency(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.Enabled = true
	config.FailureThreshold = 5
	config.SuccessThreshold = 3
	config.Timeout = 50 * time.Millisecond
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	var wg sync.WaitGroup
	successCount := 0
	failureCount := 0
	var mu sync.Mutex
	
	// Run concurrent operations
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			err := cb.Call(ctx, func(ctx context.Context) error {
				// Simulate mixed success/failure
				if id%3 == 0 {
					return fmt.Errorf("failure %d", id)
				}
				return nil
			})
			
			mu.Lock()
			if err != nil {
				failureCount++
			} else {
				successCount++
			}
			mu.Unlock()
		}(i)
	}
	
	wg.Wait()
	
	// Just verify no race conditions occurred (test should not panic)
	stats := cb.Stats()
	t.Logf("Final stats: State=%s, FailureCount=%d, SuccessCount=%d", 
		stats.State, stats.FailureCount, stats.SuccessCount)
	t.Logf("Total: Success=%d, Failure=%d", successCount, failureCount)
}

func TestCircuitBreakerError(t *testing.T) {
	err := NewCircuitBreakerError("test error")
	
	if err.Error() != "test error" {
		t.Errorf("Expected error message 'test error', got '%s'", err.Error())
	}
	
	if !IsCircuitBreakerError(err) {
		t.Error("Expected IsCircuitBreakerError to return true")
	}
	
	regularErr := fmt.Errorf("regular error")
	if IsCircuitBreakerError(regularErr) {
		t.Error("Expected IsCircuitBreakerError to return false for regular error")
	}
}

func TestCircuitBreakerWithRetryIntegration(t *testing.T) {
	// Test integration between circuit breaker and retry mechanism
	config := DefaultCircuitBreakerConfig()
	config.Enabled = true
	config.FailureThreshold = 2
	config.Timeout = 50 * time.Millisecond
	
	cb := NewCircuitBreaker(config)
	
	retryConfig := DefaultRetryConfig()
	retryConfig.MaxRetries = 3
	retryer := NewRetryer(retryConfig)
	
	ctx := context.Background()
	callCount := 0
	
	// Function that fails multiple times then succeeds
	testFunc := func(ctx context.Context) error {
		callCount++
		if callCount <= 4 {
			return NewRetryableError(fmt.Errorf("retryable failure %d", callCount))
		}
		return nil
	}
	
	// This should trigger circuit breaker after retries
	err := cb.Call(ctx, func(ctx context.Context) error {
		return retryer.Retry(ctx, testFunc)
	})
	
	// Should get an error (either retry exhausted or circuit breaker)
	if err == nil {
		t.Error("Expected error from failed calls")
	}
	
	t.Logf("Final call count: %d, Error: %v", callCount, err)
}