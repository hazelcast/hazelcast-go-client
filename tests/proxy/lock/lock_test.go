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

package lock

import (
	"log"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/rc"
	. "github.com/hazelcast/hazelcast-go-client/tests"
)

var mainClient, secondaryClient hazelcast.IHazelcastInstance
var lock core.ILock
var lockName = "ClientLockTest"

func TestMain(m *testing.M) {
	remoteController, err := NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	mainClient, _ = hazelcast.NewHazelcastClient()
	secondaryClient, _ = hazelcast.NewHazelcastClient()

	lock, _ = mainClient.GetLock(lockName)
	lock.ForceUnlock()

	m.Run()
	mainClient.Shutdown()
	secondaryClient.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestLockProxy_Lock(t *testing.T) {
	counterLock, _ := secondaryClient.GetLock(lockName)

	defer func() {
		lock.Unlock()
		counterLock.Unlock()
	}()

	err := lock.Lock()
	AssertErrorNil(t, err)

	lockChan := make(chan bool)
	timer := time.NewTimer(250 * time.Millisecond)

	go func() {
		counterLock.Lock()
		lockChan <- true
	}()

	select {
	case <-lockChan:
		t.Fatal("lock Lock() acquired on both clients")
	case <-timer.C:
	}
}

func TestLockProxy_IsLocked(t *testing.T) {
	defer lock.Unlock()

	locked, err := lock.IsLocked()
	AssertEqualf(t, err, locked, false, "lock IsLocked() returned wrong value")

	lock.Lock()

	locked, err = lock.IsLocked()
	AssertEqualf(t, err, locked, true, "lock IsLocked() returned wrong value")
}

func TestLockProxy_Unlock(t *testing.T) {
	defer lock.Unlock()

	lock.Lock()

	locked, err := lock.IsLocked()
	AssertEqualf(t, err, locked, true, "lock Lock() failed")

	lock.Unlock()

	locked, err = lock.IsLocked()
	AssertEqualf(t, err, locked, false, "lock Unlock() failed")
}

func TestLockProxy_LockWithLease(t *testing.T) {
	defer lock.Unlock()

	lock.LockWithLeaseTime(2 * time.Second)

	time.Sleep(1 * time.Second)

	locked, err := lock.IsLocked()
	AssertEqualf(t, err, locked, true, "lock LockWithLease() failed")

	time.Sleep(1 * time.Second)

	locked, err = lock.IsLocked()
	AssertEqualf(t, err, locked, false, "lock LockWithLease() failed")
}

func TestLockProxy_GetLockCount(t *testing.T) {
	lock, _ := mainClient.GetLock(lockName)
	defer lock.Unlock()

	lock.Lock()
	lock.Lock()
	lock.Lock()

	count, err := lock.GetLockCount()
	AssertEqualf(t, err, count, int32(3), "lock GetLockCount() failed")
}

func TestLockProxy_GetRemainingLeaseTime(t *testing.T) {
	defer lock.Unlock()

	lock.LockWithLeaseTime(5 * time.Second)
	time.Sleep(1500 * time.Millisecond)

	leaseTime, err := lock.GetRemainingLeaseTime()
	leaseTimeSeconds := int(leaseTime / time.Second)

	AssertEqualf(t, err, leaseTimeSeconds, 3, "lock GetRemainingLeaseTime() failed")
}

func TestLockProxy_ForceUnlock(t *testing.T) {
	defer lock.Unlock()

	counterLock, _ := secondaryClient.GetLock(lockName)

	lock.Lock()

	counterLock.ForceUnlock()

	locked, err := lock.IsLocked()
	AssertEqualf(t, err, locked, false, "lock ForceUnlock() failed")
}

func TestLockProxy_TryLock(t *testing.T) {
	defer lock.Unlock()

	locked, err := lock.TryLock()
	AssertEqualf(t, err, locked, true, "lock TryLock() failed")

	counterLock, _ := secondaryClient.GetLock(lockName)

	locked, err = counterLock.TryLock()
	AssertEqualf(t, err, locked, false, "lock TryLock() failed")
}

func TestLockProxy_TryLockWithTimeout(t *testing.T) {
	counterLock, _ := secondaryClient.GetLock(lockName)

	defer func() {
		lock.Unlock()
		counterLock.Unlock()
	}()

	lock.Lock()

	go func() {
		time.Sleep(500 * time.Millisecond)
		lock.Unlock()
	}()

	locked, err := counterLock.TryLockWithTimeout(1 * time.Second)
	AssertEqualf(t, err, locked, true, "lock TryLockWithTimeout() failed")
}

func TestLockProxy_TryLockWithTimeoutAndLease(t *testing.T) {
	counterLock, _ := secondaryClient.GetLock(lockName)

	defer func() {
		lock.Unlock()
		counterLock.Unlock()
	}()

	lock.Lock()

	go func() {
		time.Sleep(500 * time.Millisecond)
		lock.Unlock()
	}()

	locked, err := counterLock.TryLockWithTimeoutAndLease(1*time.Second, 500*time.Millisecond)
	AssertEqualf(t, err, locked, true, "lock TryLockWithTimeoutAndLease() failed")

	time.Sleep(1 * time.Second)
	locked, err = counterLock.IsLocked()
	AssertEqualf(t, err, locked, false, "lock TryLockWithTimeoutAndLease() didn't set lease time")
}
