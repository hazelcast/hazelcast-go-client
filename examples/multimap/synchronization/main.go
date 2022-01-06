/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
)

// Get a random map.
var mapName = fmt.Sprintf("sample-%d", rand.Int())

const key = "syncKey"

func main() {
	// Let's have two clients to simulate concurrent operations.
	ctx := context.TODO()
	m := createClientAndMultiMap(mapName)
	defer m.Destroy(ctx)
	// The one on the go routine will behave like a light switch, locking/unlocking everytime we signal it.
	switchClient := createClientAndMultiMap(mapName)
	signal := make(chan struct{})
	go locker(switchClient, signal)
	// Signal to lock the key. Note that we don't have to insert a key to lock on.
	signal <- struct{}{}
	time.Sleep(time.Second)
	// Observe that key is locked.
	locked, err := m.IsLocked(ctx, key)
	if err != nil {
		return
	}
	fmt.Printf("key: %s, isLocked: %t\n", key, locked)
	// Try to add an entry to this key. Use a timeout, otherwise it will block indefinitely.
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	ok, err := m.Put(ctxTimeout, key, "value")
	// Expect failed operation, context timeout error
	fmt.Printf("[client2] operation: add_new_value, succeed: %t, err: %s.\n", ok, err)
	// Try to acquire the lock. It will fail since it is already locked.
	ctx = m.NewLockContext(ctx)
	gotLock, err := m.TryLock(ctx, key)
	if err != nil {
		return
	}
	fmt.Println("lock acquired: ", gotLock)
	// Signal to unlock key.
	signal <- struct{}{}
	time.Sleep(time.Second)
	// TryLock with lease. After that, signal "locker" to observe it can lock after 2 secs lease time.
	// You can also use LockWithLease if you want to block until it is free.
	gotLock, err = m.TryLockWithLease(ctx, key, 2*time.Second)
	if err != nil {
		return
	}
	fmt.Println("lock acquired with lease: ", gotLock)
	// Use lock context "ctx" to do operations while holding the lock.
	if _, err = m.Put(ctx, key, "value"); err != nil {
		log.Fatal(err)
	}
	signal <- struct{}{}
	// Wait for "locker" to acquire the lock.
	time.Sleep(time.Second * 3)
	// Let's close the "locker" while holding the lock.
	close(signal)
	// Key is locked and the owner terminated. We cannot unlock it with "Unlock" if we are not the owner of the lock.
	// ForceUnlock it.
	if err = m.ForceUnlock(ctx, key); err != nil {
		log.Fatal(err)
	}
	// Lock is removed, we can do operations on the key.
	if err = m.Delete(ctx, key); err != nil {
		log.Fatal(err)
	}
}

// Keeps state of lock, locks/unlocks everytime when a signal is received.
func locker(m *hazelcast.MultiMap, signal chan struct{}) {
	ctx := m.NewLockContext(context.TODO())
	var isMultiMapLocked bool
	for range signal {
		var (
			lockMsg string
			err     error
		)
		if isMultiMapLocked {
			err = m.Unlock(ctx, key)
			lockMsg = "unlocked"
		} else {
			err = m.Lock(ctx, key)
			lockMsg = "locked"
		}
		if err != nil {
			panic("error encountered during lock/unlock")
		}
		isMultiMapLocked = !isMultiMapLocked
		fmt.Printf("[locker] Key \"%s\" in map \"%s\" is %s.\n", key, mapName, lockMsg)
	}
	fmt.Println("[locker] exit.")
}

func createClientAndMultiMap(mapName string) *hazelcast.MultiMap {
	ctx := context.TODO()
	// Init client and create a map.
	c1, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	m, err := c1.GetMultiMap(ctx, mapName)
	if err != nil {
		log.Fatal(err)
	}
	return m
}
