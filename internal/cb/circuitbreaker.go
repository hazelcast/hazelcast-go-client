package cb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	StateOpen   = int32(0)
	StateClosed = int32(2)
)

type TryHandler func(ctx context.Context) (interface{}, error)
type EventHandler func(state int32)
type RetryPolicyFunc func(currentTry int) time.Duration

type CircuitBreaker struct {
	// config
	MaxRetries         int
	MaxFailureCount    int32
	ResetTimeout       time.Duration
	RetryPolicyFunc    RetryPolicyFunc
	StateChangeHandler EventHandler
	// state
	CurrentFailureCount int32
	State               int32
	StateMu             *sync.RWMutex
}

func NewCircuitBreaker(fs ...CircuitBreakerOptionFunc) *CircuitBreaker {
	opts, err := NewCircuitBreakerOptions(fs...)
	if err != nil {
		panic(fmt.Errorf("evaluating circuitbreaker options: %w", err))
	}
	retryPolicyFunc := opts.RetryPolicyFunc
	if retryPolicyFunc == nil {
		retryPolicyFunc = func(trial int) time.Duration {
			return time.Duration(trial) * time.Millisecond
		}
	}
	return &CircuitBreaker{
		MaxRetries:         opts.MaxRetries,
		MaxFailureCount:    opts.MaxFailureCount,
		ResetTimeout:       opts.ResetTimeout,
		RetryPolicyFunc:    retryPolicyFunc,
		StateChangeHandler: opts.StateChangeHandler,
		State:              StateClosed,
		StateMu:            &sync.RWMutex{},
	}
}

func (cb *CircuitBreaker) Try(tryHandler TryHandler) Future {
	return cb.TryWithContext(context.Background(), tryHandler)
}

func (cb *CircuitBreaker) TryWithContext(ctx context.Context, tryHandler TryHandler) Future {
	cb.StateMu.RLock()
	state := cb.State
	cb.StateMu.RUnlock()
	if state == StateOpen {
		return NewFailedFuture(ErrCircuitOpen)
	}
	future := NewFutureImpl()
	go cb.try(ctx, future.resultCh, tryHandler)
	return future
}

func (cb *CircuitBreaker) try(ctx context.Context, resultCh chan interface{}, tryHandler TryHandler) {
	var result interface{}
	var err error
loop:
	for trial := 0; trial <= cb.MaxRetries; trial++ {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break loop
		default:
			if result, err = tryHandler(ctx); err == nil {
				// succeeded
				break loop
			}
			if trial < cb.MaxRetries {
				time.Sleep(cb.RetryPolicyFunc(trial))
			}
		}
	}
	if err != nil {
		// failed
		cb.notifyFailed()
		result = err
	}
	resultCh <- result
	close(resultCh)
}

func (cb *CircuitBreaker) notifyFailed() {
	failureCount := atomic.AddInt32(&cb.CurrentFailureCount, 1)
	if failureCount > cb.MaxFailureCount {
		cb.openCircuit()
	}
}

func (cb *CircuitBreaker) openCircuit() {
	cb.StateMu.Lock()
	if state := cb.State; state == StateOpen {
		cb.StateMu.Unlock()
		// state is already open, don't change it
		return
	}
	state := StateOpen
	cb.State = state
	cb.StateMu.Unlock()
	if cb.StateChangeHandler != nil {
		cb.StateChangeHandler(state)
	}
	go func(resetTimeout time.Duration) {
		// close the circuit after reset timeout
		time.Sleep(resetTimeout)
		cb.closeCircuit()
	}(cb.ResetTimeout)
}

func (cb *CircuitBreaker) closeCircuit() {
	cb.StateMu.Lock()
	defer cb.StateMu.Unlock()
	if state := cb.State; state == StateClosed {
		// state is closed, don't change it
		return
	}
	cb.State = StateClosed
	// TODO:
	atomic.StoreInt32(&cb.CurrentFailureCount, 0)
	if cb.StateChangeHandler != nil {
		cb.StateChangeHandler(cb.State)
	}
}
