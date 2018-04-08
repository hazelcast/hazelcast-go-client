// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import "time"

// ILock is the distributed implementation of re-entrant lock,
// meaning that if you lock using an ILock,  the critical section that it guards is guaranteed
// to be executed by only one thread in the entire cluster, provided that there are no network failures.
// Even though locks are great for synchronization, they can lead to problems if not used properly.
// Also note that Hazelcast Lock does not support fairness.
type ILock interface {
	// IDistributedObject is the base interface for all distributed objects.
	IDistributedObject

	// IsLocked checks the lock status.
	// If the lock is acquired, it returns true. Otherwise, it returns false.
	IsLocked() (locked bool, err error)

	// IsLocked checks if the lock is acquired by the current thread.
	// If the lock is acquired by the current thread, it returns true. Otherwise, it returns false.
	IsLockedByCurrentThread() (locked bool, err error)

	// GetLockCount returns the number of re-entrant lock hold count, regardless of lock ownership.
	GetLockCount() (count int32, err error)

	// GetRemainingLeaseTime returns the remaining lease time.
	// If the lock is not acquired, then -1 is returned.
	GetRemainingLeaseTime() (leaseTime time.Duration, err error)

	// Lock acquires the lock.
	// If the lock is not available, then
	// the current thread becomes disabled for thread scheduling
	// purposes and lies dormant until the lock has been acquired.
	// Locks are re-entrant, so if the key is locked N times then
	// it should be unlocked N times before another thread can acquire it.
	Lock() (err error)

	// LockWithLeaseTime acquires the lock for the specified lease time.
	// After lease time, the lock will be released.
	// If the lock is not available, then
	// the current thread becomes disabled for thread scheduling
	// purposes and lies dormant until the lock has been acquired.
	// Locks are re-entrant, so if the key is locked N times then
	// it should be unlocked N times before another thread can acquire it.
	LockWithLeaseTime(leaseTime time.Duration) (err error)

	// Unlock releases the lock. It never blocks and returns immediately.
	// If the current thread is the holder of this lock,
	// then the hold count is decremented. If the hold count is zero, then the lock is released.
	Unlock() (err error)

	// ForceUnlock releases the lock regardless of the lock owner.
	// It always successfully unlocks the key, never blocks, and returns immediately.
	ForceUnlock() (err error)

	// TryLock tries to acquire the lock.
	// If the lock is not available then the current thread
	// does not wait and returns false immediately.
	// TryLock returns true if lock is acquired, false otherwise.
	TryLock() (locked bool, err error)

	// TryLockWithTimeout tries to acquire the lock.
	// If the lock is not available, then
	// the current thread becomes disabled for thread scheduling
	// purposes and lies dormant until one of two things happens:
	//	the lock is acquired by the current thread, or
	//	the specified waiting time elapses.
	// TryLockWithTimeout returns true if lock is acquired, false otherwise.
	TryLockWithTimeout(timeout time.Duration) (locked bool, err error)

	// TryLockWithTimeoutAndLease tries to acquire the lock for the specified lease time.
	// After lease time, the lock will be released.
	// If the lock is not available, then
	// the current thread becomes disabled for thread scheduling
	// purposes and lies dormant until one of two things happens:
	//	the lock is acquired by the current thread, or
	//	the specified waiting time elapses.
	// TryLockWithTimeoutAndLease returns true if lock is acquired, false ot
	TryLockWithTimeoutAndLease(timeout time.Duration, leaseTime time.Duration) (locked bool, err error)
}
