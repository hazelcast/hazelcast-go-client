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

type ILock interface {
	// IDistributedObject is the base interface for all distributed objects.
	IDistributedObject

	IsLocked() (locked bool, err error)

	IsLockedByCurrentThread() (locked bool, err error)

	GetLockCount() (count int32, err error)

	GetRemainingLeaseTime() (leaseTime time.Duration, err error)

	Lock() (err error)

	LockWithLeaseTime(leaseTime time.Duration) (err error)

	Unlock() (err error)

	ForceUnlock() (err error)

	TryLock() (locked bool, err error)

	TryLockWithTimeout(timeout time.Duration) (locked bool, err error)

	TryLockWithTimeoutAndLease(timeout time.Duration, leaseTime time.Duration) (locked bool, err error)
}
