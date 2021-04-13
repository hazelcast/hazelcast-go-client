package cb_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/cb"
)

func TestFailingCircuitBreaker(t *testing.T) {
	targetErr := errors.New("ERR")
	c := cb.NewCircuitBreaker(cb.MaxFailureCount(3), cb.MaxRetries(2))
	tries := 0
	fut := c.Try(func(ctx context.Context) (interface{}, error) {
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
	c := cb.NewCircuitBreaker(cb.MaxFailureCount(0))
	failingFunc := func(ctx context.Context) (interface{}, error) {
		return nil, targetErr
	}
	targetVal := "ok"
	succeedingFunc := func(ctx context.Context) (interface{}, error) {
		return targetVal, nil
	}
	fut := c.Try(failingFunc)
	if _, err := fut.Result(); err == nil {
		t.Fatal("result must return error")
	} else if targetErr != err {
		t.Fatalf("target err: %v != %v", targetErr, err)
	}
	// check that circuit was opened
	fut = c.Try(succeedingFunc)
	if _, err := fut.Result(); err == nil {
		t.Fatalf("result must return error")
	} else if cb.ErrCircuitOpen != err {
		t.Fatalf("target err: %v != %v", cb.ErrCircuitOpen, err)
	}
	// check that circuit is closed after a while
	time.Sleep(3 * time.Second)
	fut = c.Try(succeedingFunc)
	if val, err := fut.Result(); err != nil {
		t.Fatal(err)
	} else if targetVal != val {
		t.Fatalf("target val: %v != %v", targetVal, val)
	}
	// check that circuit failure count was reset
	fut = c.Try(failingFunc)
	if _, err := fut.Result(); err == nil {
		t.Fatalf("should have failed")
	} else if targetErr != err {
		t.Fatalf("target err: %v != %v", targetErr, err)
	}
	fut = c.Try(failingFunc)
	if _, err := fut.Result(); err == nil {
		t.Fatalf("should have failed")
	} else if cb.ErrCircuitOpen != err {
		t.Fatalf("target err: %v != %v", cb.ErrCircuitOpen, err)
	}
}

func TestSucceedingCircuitBreaker(t *testing.T) {
	targetVal := "ok"
	c := cb.NewCircuitBreaker(cb.MaxFailureCount(3), cb.MaxRetries(2))
	fut := c.Try(func(ctx context.Context) (interface{}, error) {
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
	c.TryWithContext(myCtx, func(ctx context.Context) (interface{}, error) {
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
