/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	StateOpen   = int32(0)
	StateClosed = int32(2)
)

type TryHandler func(ctx context.Context, attempt int) (interface{}, error)
type EventHandler func(state int32)
type RetryPolicyFunc func(currentTry int) time.Duration

type CircuitBreaker struct {
	Deadline            time.Time
	TimeoutText         string
	RetryPolicyFunc     RetryPolicyFunc
	StateChangeHandler  EventHandler
	MaxRetries          int
	ResetTimeout        time.Duration
	SleepDuration       time.Duration
	MaxFailureCount     int32
	CurrentFailureCount int32
	State               int32
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
		TimeoutText:        opts.TimeoutText,
		Deadline:           MakeDeadline(opts.Timeout),
		RetryPolicyFunc:    retryPolicyFunc,
		StateChangeHandler: opts.StateChangeHandler,
		State:              StateClosed,
	}
}

func (cb *CircuitBreaker) Try(handler TryHandler) (interface{}, error) {
	return cb.TryContext(context.Background(), handler)
}

func (cb *CircuitBreaker) TryContext(ctx context.Context, handler TryHandler) (interface{}, error) {
	if state := atomic.LoadInt32(&cb.State); state == StateOpen {
		return nil, ErrCircuitOpen
	}
	return cb.try(ctx, handler)
}

func (cb *CircuitBreaker) TryContextFuture(ctx context.Context, tryHandler TryHandler) Future {
	if state := atomic.LoadInt32(&cb.State); state == StateOpen {
		return NewFailedFuture(ErrCircuitOpen)
	}
	future := NewFutureImpl()
	cb.tryChan(ctx, future.resultCh, tryHandler)
	return future
}

func (cb *CircuitBreaker) tryChan(ctx context.Context, resultCh chan interface{}, tryHandler TryHandler) {
	if result, err := cb.try(ctx, tryHandler); err != nil {
		resultCh <- err
	} else {
		resultCh <- result
	}
	close(resultCh)
}

func (cb *CircuitBreaker) try(ctx context.Context, tryHandler TryHandler) (result interface{}, err error) {
	var nonRetryableErr *NonRetryableError
loop:
	for attempt := 0; attempt <= cb.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break loop
		default:
			if time.Now().After(cb.Deadline) {
				err = ErrDeadlineExceeded
				break loop
			}
			result, err = tryHandler(ctx, attempt)
			if err == nil || contextErr(err) {
				break loop
			}
			if errors.As(err, &nonRetryableErr) {
				err = nonRetryableErr.Err
				break loop
			}
			if attempt < cb.MaxRetries {
				time.Sleep(cb.SleepDuration)
			}
		}
	}
	if err != nil && !contextErr(err) {
		cb.notifyFailed()
	}
	return result, err
}

func (cb *CircuitBreaker) notifyFailed() {
	failureCount := atomic.AddInt32(&cb.CurrentFailureCount, 1)
	if failureCount > cb.MaxFailureCount {
		if cb.ResetTimeout > 0 {
			cb.openCircuit()
		} else {
			cb.reset()
		}
	}
}

func (cb *CircuitBreaker) openCircuit() {
	if !atomic.CompareAndSwapInt32(&cb.State, StateClosed, StateOpen) {
		return
	}
	if cb.StateChangeHandler != nil {
		cb.StateChangeHandler(StateOpen)
	}
	go func(resetTimeout time.Duration) {
		// close the circuit after reset timeout
		time.Sleep(resetTimeout)
		cb.closeCircuit()
	}(cb.ResetTimeout)
}

func (cb *CircuitBreaker) closeCircuit() {
	if !atomic.CompareAndSwapInt32(&cb.State, StateOpen, StateClosed) {
		return
	}
	cb.reset()
	if cb.StateChangeHandler != nil {
		cb.StateChangeHandler(StateClosed)
	}
}

func (cb *CircuitBreaker) reset() {
	atomic.StoreInt32(&cb.CurrentFailureCount, 0)
}

func contextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func MakeDeadline(timeout time.Duration) time.Time {
	// limit timeout so it doesn't overflow the deadline
	now := time.Now()
	maxTime := time.Unix(1<<63-62135596801, 999999999)
	maxTimeout := maxTime.Sub(now)
	if maxTimeout <= timeout {
		return maxTime
	}
	return now.Add(timeout)
}
