package cp

import (
	"context"
)

// AtomicLong is a redundant and highly available distributed counter
// for 64-bit integers (``long`` type in Java).
// It works on top of the Raft consensus algorithm. It offers linearizability
// during crash failures and network partitions. It is CP with respect to
// the CAP principle. If a network partition occurs, it remains available
// on at most one side of the partition.
// AtomicLong implementation does not offer exactly-once / effectively-once
// execution semantics. It goes with at-least-once execution semantics
// by default and can cause an API call to be committed multiple times
// in case of CP member failures. It can be tuned to offer at-most-once
// execution semantics. Please see `fail-on-indeterminate-operation-state`
// server-side setting.
type AtomicLong interface {
	proxy
	// AddAndGet atomically adds the given value to the current value.
	AddAndGet(ctx context.Context, delta int64) (int64, error)
	// CompareAndSet Atomically sets the value to the given updated value
	// only if the current value equals the expected value.
	CompareAndSet(ctx context.Context, expect int64, update int64) (bool, error)
	// Get gets the current value.
	Get(ctx context.Context) (int64, error)
	// GetAndAdd atomically adds the given value to the current value.
	GetAndAdd(ctx context.Context, delta int64) (int64, error)
	// GetAndSet atomically sets the given value and returns the old value.
	GetAndSet(ctx context.Context, value int64) (int64, error)
	// Set atomically sets the given value.
	Set(ctx context.Context, value int64) error
	// IncrementAndGet atomically increments the current value by one.
	IncrementAndGet(ctx context.Context) (int64, error)
	// DecrementAndGet atomically decrements the current value by one.
	DecrementAndGet(ctx context.Context) (int64, error)
	// GetAndDecrement atomically decrements the current value by one.
	GetAndDecrement(ctx context.Context) (int64, error)
	// GetAndIncrement atomically increments the current value by one.
	GetAndIncrement(ctx context.Context) (int64, error)
	// Apply applies a function on the value, the actual stored value will not change.
	Apply(ctx context.Context, function interface{}) (interface{}, error)
	// Alter alters the currently stored value by applying a function on it.
	Alter(ctx context.Context, function interface{}) error
	// GetAndAlter alters the currently stored value by applying a function on it and gets the old value.
	GetAndAlter(ctx context.Context, function interface{}) (int64, error)
	// AlterAndGet alters the currently stored value by applying a function on it and gets the result.
	AlterAndGet(ctx context.Context, function interface{}) (int64, error)
}
