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
	"sync"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	ctx := context.TODO()
	c, m := createClientAndMultiMap()
	defer c.Shutdown(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	const key = "foo"
	// Let's use go routines to simulate other clients/processes.
	// "key" will be locked by other process for two seconds.
	go func() {
		ctx := m.NewLockContext(ctx)
		must(m.LockWithLease(ctx, key, 2*time.Second))
		fmt.Println("[other process] has the lock")
		wg.Done()
	}()
	wg.Wait()
	lockCtx := m.NewLockContext(ctx)
	// Try to acquire the lock. It fails, key is locked.
	ok := mustBool(m.TryLock(lockCtx, key))
	fmt.Printf("operation: TryLockWith, succeed: %t\n", ok)
	// Try to acquire the lock again for 3 seconds. This time it will out run the other process and acquire it.
	ok = mustBool(m.TryLockWithTimeout(lockCtx, key, 3*time.Second))
	fmt.Printf("operation: TryLockWithTimeout, succeed: %t\n", ok)
	// Do an operation holding the lock, ignore the previous value.
	_, err := m.Put(lockCtx, key, "test")
	must(err)
	// Another process may try to acquire lock.
	wg.Add(1)
	go func() {
		ctx := m.NewLockContext(ctx)
		// Try to acquire lock for a second to hold it for 2 seconds. It fails, we have the lock.
		ok := mustBool(m.TryLockWithLeaseAndTimeout(ctx, key, 2*time.Second, time.Millisecond))
		fmt.Printf("[other process] operation: TryLockWithLeaseAndTimeout, succeed: %t\n", ok)
		wg.Done()
	}()
	wg.Wait()
	// Release the lock
	must(m.Unlock(lockCtx, key))
	must(m.Delete(lockCtx, key))
}

func createClientAndMultiMap() (*hazelcast.Client, *hazelcast.MultiMap) {
	ctx := context.TODO()
	// Init client and create a map.
	c, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Get a random map name.
	mapName := fmt.Sprintf("sample-%d", rand.Int())
	m, err := c.GetMultiMap(ctx, mapName)
	must(err)
	return c, m
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func mustBool(value bool, err error) bool {
	must(err)
	return value
}
