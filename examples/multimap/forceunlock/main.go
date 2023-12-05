/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
	// "key" is locked by another process, and it terminated before unlocking it.
	go func() {
		ctx := hazelcast.NewLockContext(ctx)
		if err := m.Lock(ctx, key); err != nil {
			panic(err)
		}
		fmt.Println("[other process] has the lock and is terminating...")
		wg.Done()
	}()
	wg.Wait()
	// You can still unlock the key, even if you are not the lock owner.
	lockCtx := hazelcast.NewLockContext(ctx)
	if err := m.ForceUnlock(lockCtx, key); err != nil {
		panic(err)
	}
	// Key must be unlocked.
	locked, err := m.IsLocked(ctx, key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("key isLocked:%t", locked)
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
	if err != nil {
		log.Fatal(err)
	}
	return c, m
}
