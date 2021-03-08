package cb

import (
	"errors"
	"math"
	"time"
)

type CircuitBreakerOptions struct {
	MaxRetries         int
	MaxFailureCount    int32
	ResetTimeout       time.Duration
	RetryPolicyFunc    RetryPolicyFunc
	StateChangeHandler EventHandler
}

func NewCircuitBreakerOptions(fs ...CircuitBreakerOptionFunc) (*CircuitBreakerOptions, error) {
	opts := DefaultCircuitBreakerOptions()
	for _, optFun := range fs {
		if err := optFun(opts); err != nil {
			return nil, err
		}
	}
	return opts, nil
}

type CircuitBreakerOptionFunc func(opts *CircuitBreakerOptions) error

func DefaultCircuitBreakerOptions() *CircuitBreakerOptions {
	return &CircuitBreakerOptions{
		MaxRetries:      0,
		MaxFailureCount: 0,
		ResetTimeout:    1 * time.Second,
		RetryPolicyFunc: func(tries int) time.Duration {
			return 0
		},
	}
}

func MaxRetries(retries int) CircuitBreakerOptionFunc {
	return func(opts *CircuitBreakerOptions) error {
		if retries < 0 {
			return errors.New("MaxRetries must be non-negative")
		}
		opts.MaxRetries = retries
		return nil
	}
}

func MaxFailureCount(failureCount int) CircuitBreakerOptionFunc {
	return func(opts *CircuitBreakerOptions) error {
		if failureCount < 0 {
			return errors.New("MaxFailureCount must be non-negative")
		}
		if failureCount > math.MaxInt32 {
			return errors.New("MaxFailureCount must fit into int32")
		}
		opts.MaxFailureCount = int32(failureCount)
		return nil
	}
}

func ResetTimeout(timeout time.Duration) CircuitBreakerOptionFunc {
	return func(opts *CircuitBreakerOptions) error {
		opts.ResetTimeout = timeout
		return nil
	}
}

func RetryPolicy(policyFunc RetryPolicyFunc) CircuitBreakerOptionFunc {
	return func(opts *CircuitBreakerOptions) error {
		opts.RetryPolicyFunc = policyFunc
		return nil
	}
}

func StateChangeHandler(handler EventHandler) CircuitBreakerOptionFunc {
	return func(opts *CircuitBreakerOptions) error {
		opts.StateChangeHandler = handler
		return nil
	}
}
