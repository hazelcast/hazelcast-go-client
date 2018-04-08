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

package internal

import (
	"time"

	. "github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

type LockProxy struct {
	*partitionSpecificProxy
}

func newLockProxy(client *HazelcastClient, serviceName *string, name *string) (*LockProxy, error) {
	parSpecProxy, err := newPartitionSpecificProxy(client, serviceName, name)
	if err != nil {
		return nil, err
	}
	return &LockProxy{parSpecProxy}, nil
}

func (lock *LockProxy) IsLocked() (locked bool, err error) {
	request := LockIsLockedEncodeRequest(lock.name)
	responseMessage, err := lock.invoke(request)
	return lock.decodeToBoolAndError(responseMessage, err, LockIsLockedDecodeResponse)
}

func (lock *LockProxy) IsLockedByCurrentThread() (locked bool, err error) {
	request := LockIsLockedByCurrentThreadEncodeRequest(lock.name, threadId)
	responseMessage, err := lock.invoke(request)
	return lock.decodeToBoolAndError(responseMessage, err, LockIsLockedByCurrentThreadDecodeResponse)
}

func (lock *LockProxy) GetLockCount() (count int32, err error) {
	request := LockGetLockCountEncodeRequest(lock.name)
	responseMessage, err := lock.invoke(request)
	return lock.decodeToInt32AndError(responseMessage, err, LockGetLockCountDecodeResponse)
}

func (lock *LockProxy) GetRemainingLeaseTime() (leaseTime time.Duration, err error) {
	request := LockGetRemainingLeaseTimeEncodeRequest(lock.name)
	responseMessage, err := lock.invoke(request)
	if err != nil {
		return 0, nil
	}
	remainingLeaseTime := LockGetRemainingLeaseTimeDecodeResponse(responseMessage)()
	return time.Duration(remainingLeaseTime) * time.Millisecond, nil
}

func (lock *LockProxy) Lock() (err error) {
	return lock.LockWithLeaseTime(-1 * time.Millisecond)
}

func (lock *LockProxy) LockWithLeaseTime(leaseTime time.Duration) (err error) {
	leaseTimeInMilliseconds := GetDurationInMilliseconds(leaseTime)
	request := LockLockEncodeRequest(lock.name, leaseTimeInMilliseconds, threadId)
	_, err = lock.invoke(request)
	return err
}

func (lock *LockProxy) Unlock() (err error) {
	request := LockUnlockEncodeRequest(lock.name, threadId)
	_, err = lock.invoke(request)
	return err
}

func (lock *LockProxy) ForceUnlock() (err error) {
	request := LockForceUnlockEncodeRequest(lock.name)
	_, err = lock.invoke(request)
	return err
}

func (lock *LockProxy) TryLock() (locked bool, err error) {
	return lock.TryLockWithTimeout(0)
}

func (lock *LockProxy) TryLockWithTimeout(timeout time.Duration) (locked bool, err error) {
	return lock.TryLockWithTimeoutAndLease(timeout, -1*time.Millisecond)
}

func (lock *LockProxy) TryLockWithTimeoutAndLease(timeout time.Duration, leaseTime time.Duration) (locked bool, err error) {
	timeoutInMilliseconds := GetDurationInMilliseconds(timeout)
	leaseTimeInMilliseconds := GetDurationInMilliseconds(leaseTime)
	request := LockTryLockEncodeRequest(lock.name, timeoutInMilliseconds, leaseTimeInMilliseconds, threadId)
	responseMessage, err := lock.invoke(request)
	return lock.decodeToBoolAndError(responseMessage, err, LockTryLockDecodeResponse)
}
