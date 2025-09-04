package bimawen

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// RetryableError represents an error that can be retried
type RetryableError struct {
	Err       error
	Retryable bool
}

// Error implements the error interface
func (e *RetryableError) Error() string {
	return e.Err.Error()
}

// Unwrap returns the underlying error
func (e *RetryableError) Unwrap() error {
	return e.Err
}

// IsRetryable returns whether the error is retryable
func (e *RetryableError) IsRetryable() bool {
	return e.Retryable
}

// NewRetryableError creates a new retryable error
func NewRetryableError(err error) *RetryableError {
	return &RetryableError{Err: err, Retryable: true}
}

// NewNonRetryableError creates a new non-retryable error
func NewNonRetryableError(err error) *RetryableError {
	return &RetryableError{Err: err, Retryable: false}
}

// RetryFunc is the function type that can be retried
type RetryFunc func(ctx context.Context) error

// Retryer handles retry logic with exponential backoff
type Retryer struct {
	config *RetryConfig
	rand   *rand.Rand
}

// NewRetryer creates a new retryer with the given configuration
func NewRetryer(config *RetryConfig) *Retryer {
	if config == nil {
		config = DefaultRetryConfig()
	}
	
	return &Retryer{
		config: config,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Retry executes the function with exponential backoff retry logic
func (r *Retryer) Retry(ctx context.Context, fn RetryFunc) error {
	var lastErr error
	
	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		
		// Execute the function
		err := fn(ctx)
		if err == nil {
			return nil // Success
		}
		
		lastErr = err
		
		// Check if this is a retryable error
		if retryableErr, ok := err.(*RetryableError); ok && !retryableErr.IsRetryable() {
			return err // Non-retryable error, fail immediately
		}
		
		// Don't wait after the last attempt
		if attempt == r.config.MaxRetries {
			break
		}
		
		// Calculate backoff duration
		backoff := r.calculateBackoff(attempt)
		
		// Wait for backoff duration or context cancellation
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			// Continue to next attempt
		}
	}
	
	return fmt.Errorf("max retries (%d) exceeded, last error: %w", r.config.MaxRetries, lastErr)
}

// calculateBackoff calculates the backoff duration for the given attempt
func (r *Retryer) calculateBackoff(attempt int) time.Duration {
	// Calculate exponential backoff
	backoff := float64(r.config.InitialInterval) * math.Pow(r.config.Multiplier, float64(attempt))
	
	// Apply maximum interval limit
	if backoff > float64(r.config.MaxInterval) {
		backoff = float64(r.config.MaxInterval)
	}
	
	// Add randomization factor (jitter) to prevent thundering herd
	if r.config.RandomizationFactor > 0 {
		jitter := r.rand.Float64() * r.config.RandomizationFactor * backoff
		if r.rand.Intn(2) == 0 {
			backoff -= jitter
		} else {
			backoff += jitter
		}
	}
	
	// Ensure minimum backoff
	if backoff < 0 {
		backoff = float64(r.config.InitialInterval)
	}
	
	return time.Duration(backoff)
}

// RetryWithConfig is a convenience function that creates a retryer and executes the function
func RetryWithConfig(ctx context.Context, config *RetryConfig, fn RetryFunc) error {
	retryer := NewRetryer(config)
	return retryer.Retry(ctx, fn)
}

// SimpleRetry is a convenience function with default retry configuration
func SimpleRetry(ctx context.Context, fn RetryFunc) error {
	return RetryWithConfig(ctx, DefaultRetryConfig(), fn)
}

// IsRetryableError checks if an error is retryable
func IsRetryableError(err error) bool {
	if retryableErr, ok := err.(*RetryableError); ok {
		return retryableErr.IsRetryable()
	}
	// By default, consider errors as retryable unless explicitly marked as non-retryable
	return true
}

// WrapRetryableError wraps an error as retryable or non-retryable based on the error type
func WrapRetryableError(err error) error {
	if err == nil {
		return nil
	}
	
	// Check for specific error types that should not be retried
	switch {
	case IsContextError(err):
		return NewNonRetryableError(err)
	case IsAuthenticationError(err):
		return NewNonRetryableError(err)
	case IsValidationError(err):
		return NewNonRetryableError(err)
	default:
		return NewRetryableError(err)
	}
}

// IsContextError checks if the error is a context error
func IsContextError(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}

// IsAuthenticationError checks if the error is an authentication error
func IsAuthenticationError(err error) bool {
	// This is a simple implementation, you might want to check for specific error types
	errStr := err.Error()
	return contains(errStr, "authentication") || 
		   contains(errStr, "unauthorized") || 
		   contains(errStr, "access denied") ||
		   contains(errStr, "permission denied")
}

// IsValidationError checks if the error is a validation error
func IsValidationError(err error) bool {
	errStr := err.Error()
	return contains(errStr, "validation") || 
		   contains(errStr, "invalid") || 
		   contains(errStr, "malformed")
}

// contains is a helper function to check if a string contains a substring (case-insensitive)
func contains(str, substr string) bool {
	return len(str) >= len(substr) && 
		   (str == substr || 
		    (len(str) > len(substr) && 
		     func() bool {
		         for i := 0; i <= len(str)-len(substr); i++ {
		             if str[i:i+len(substr)] == substr {
		                 return true
		             }
		         }
		         return false
		     }()))
}