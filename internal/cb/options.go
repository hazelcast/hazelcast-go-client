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
	"errors"
	"fmt"
	logger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"math"
	"time"
)

const MaxDuration int64 = 1<<63 - 1

type CircuitBreakerOptions struct {
	RetryPolicyFunc    RetryPolicyFunc
	StateChangeHandler EventHandler
	MaxRetries         int
	ResetTimeout       time.Duration
	Timeout            time.Duration
	TimeoutText        string
	SleepDuration      time.Duration
	MaxBackoff         time.Duration
	MaxFailureCount    int32
	Logger             logger.LogAdaptor
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
		SleepDuration:   time.Second * 1,
		MaxRetries:      0,
		MaxFailureCount: 0,
		ResetTimeout:    0,
		MaxBackoff:      0,
		Timeout:         0,
		RetryPolicyFunc: func(tries int) time.Duration {
			return 0
		},
	}
}

func Logger(logger logger.LogAdaptor) CircuitBreakerOptionFunc {
	return func(opts *CircuitBreakerOptions) error {
		if logger.Logger == nil {
			return errors.New("LogAdaptor must have a value")
		}
		opts.Logger = logger
		return nil
	}
}
func MaxBackoff(backoff time.Duration) CircuitBreakerOptionFunc {
	return func(opts *CircuitBreakerOptions) error {
		if backoff < 0 {
			return errors.New("MaxBackoff must be non-negative")
		}
		opts.MaxBackoff = backoff
		return nil
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

func Timeout(timeout time.Duration) CircuitBreakerOptionFunc {
	return func(opts *CircuitBreakerOptions) error {
		opts.Timeout = timeout
		if timeout.Nanoseconds() == MaxDuration {
			opts.TimeoutText = "INFINITE"
		} else {
			opts.TimeoutText = fmt.Sprintf("%.2fs", timeout.Seconds())
		}
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
