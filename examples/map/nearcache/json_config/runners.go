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
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client"
)

type Runner interface {
	Run(cfg Config, m *hazelcast.Map) (operationCount int64)
}

type SimpleRunner struct{}

func (s SimpleRunner) Run(cfg Config, m *hazelcast.Map) (operationCount int64) {
	if cfg.GoroutineCount > 1 {
		return s.doParallel(cfg, m)
	}
	return s.doSequential(cfg, m)
}

func (s SimpleRunner) doSequential(cfg Config, m *hazelcast.Map) int64 {
	for i := int64(0); i < int64(cfg.KeyCount); i++ {
		if _, err := m.Get(context.Background(), i); err != nil {
			panic(err)
		}
	}
	return int64(cfg.KeyCount)
}

func (s SimpleRunner) doParallel(cfg Config, m *hazelcast.Map) int64 {
	kc := cfg.KeyCount
	gc := cfg.GoroutineCount
	if kc%gc != 0 {
		panic("configuration KeyCount must be multiples of GoroutineCount")
	}
	keyPerGoroutine := kc / gc
	wg := &sync.WaitGroup{}
	wg.Add(gc)
	var opCnt int64
	for i := 0; i < gc; i += 1 {
		go func(start, count int64, wg *sync.WaitGroup) {
			for v := start; v < start+count; v++ {
				if _, err := m.Get(context.Background(), v); err != nil {
					panic(err)
				}
			}
			wg.Done()
			atomic.AddInt64(&opCnt, count)
		}(int64(i*keyPerGoroutine), int64(keyPerGoroutine), wg)
	}
	wg.Wait()
	return opCnt
}
