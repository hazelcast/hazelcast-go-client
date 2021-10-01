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

package cb_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/cb"
)

func TestFailingCircuitBreaker(t *testing.T) {
	targetErr := errors.New("ERR")
	c := cb.NewCircuitBreaker(cb.MaxFailureCount(3), cb.MaxRetries(2))
	tries := 0
	fut := c.TryContextFuture(context.Background(), func(ctx context.Context, attempt int) (interface{}, error) {
		tries++
		return nil, targetErr
	})
	if _, err := fut.Result(); err == nil {
		t.Fatal("result must return error")
	} else {
		if targetErr != err {
			t.Fatalf("target err: %v != %v", targetErr, err)
		}
	}
	targetTries := 3
	if targetTries != tries {
		t.Fatalf("target tries: %d != %d", targetTries, tries)
	}
}

func TestFailingCircuitBreakerCircuitOpen(t *testing.T) {
	targetErr := errors.New("ERR")
	c := cb.NewCircuitBreaker(cb.MaxFailureCount(0), cb.ResetTimeout(1*time.Second))
	failingFunc := func(ctx context.Context, attempt int) (interface{}, error) {
		return nil, targetErr
	}
	targetVal := "ok"
	succeedingFunc := func(ctx context.Context, attempt int) (interface{}, error) {
		return targetVal, nil
	}
	if _, err := c.Try(failingFunc); err == nil {
		t.Fatal("result must return error")
	} else if targetErr != err {
		t.Fatalf("target err: %v != %v", targetErr, err)
	}
	// check that circuit was opened
	if _, err := c.Try(succeedingFunc); err == nil {
		t.Fatalf("result must return error")
	} else if cb.ErrCircuitOpen != err {
		t.Fatalf("target err: %v != %v", cb.ErrCircuitOpen, err)
	}
	// check that circuit is closed after a while
	time.Sleep(3 * time.Second)
	if val, err := c.Try(succeedingFunc); err != nil {
		t.Fatal(err)
	} else if targetVal != val {
		t.Fatalf("target val: %v != %v", targetVal, val)
	}
	// check that circuit failure count was reset
	if _, err := c.Try(failingFunc); err == nil {
		t.Fatalf("should have failed")
	} else if targetErr != err {
		t.Fatalf("target err: %v != %v", targetErr, err)
	}
	if _, err := c.Try(failingFunc); err == nil {
		t.Fatalf("should have failed")
	} else if cb.ErrCircuitOpen != err {
		t.Fatalf("target err: %v != %v", cb.ErrCircuitOpen, err)
	}
}

func TestSucceedingCircuitBreaker(t *testing.T) {
	targetVal := "ok"
	c := cb.NewCircuitBreaker(cb.MaxFailureCount(3), cb.MaxRetries(2))
	fut := c.TryContextFuture(context.Background(), func(ctx context.Context, attempt int) (interface{}, error) {
		return targetVal, nil
	})
	if val, err := fut.Result(); err != nil {
		t.Fatal(err)
	} else {
		if targetVal != val {
			t.Fatalf("target val: %v != %v", targetVal, val)
		}
	}
}

func TestCancelFuture(t *testing.T) {
	c := cb.NewCircuitBreaker(cb.MaxFailureCount(3), cb.MaxRetries(2))
	funcCancelled := int32(0)
	myCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	c.TryContextFuture(myCtx, func(ctx context.Context, attempt int) (interface{}, error) {
		for {
			select {
			case <-ctx.Done():
				atomic.StoreInt32(&funcCancelled, 1)
				return nil, ctx.Err()
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	})
	time.Sleep(1 * time.Second)
	cancel()
	time.Sleep(200 * time.Millisecond)
	if atomic.LoadInt32(&funcCancelled) != 1 {
		t.Fatalf("try handler was not cancelled")
	}
}

func TestMakeDeadline(t *testing.T) {
	deadline := cb.MakeDeadline(time.Duration(cb.MaxDuration))
	assert.Equal(t, time.Unix(1<<63-62135596801, 999999999), deadline)

	now := time.Now()
	deadline = cb.MakeDeadline(100 * time.Hour)
	assert.InDelta(t, now.Add(100*time.Hour).UnixNano(), deadline.UnixNano(), 1_000_000)
}
