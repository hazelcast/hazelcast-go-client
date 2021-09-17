/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package event_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

type sampleEvent struct {
	value int
}

func (e sampleEvent) EventName() string {
	return "sample.event"
}

func TestDispatchServiceSubscribePublish(t *testing.T) {
	goroutineCount := 10000
	wg := &sync.WaitGroup{}
	wg.Add(goroutineCount)
	dispatchCount := int32(0)
	handler := func(event event.Event) {
		atomic.AddInt32(&dispatchCount, 1)
		wg.Done()
	}
	service := event.NewDispatchService(logger.New())
	service.Subscribe("sample.event", 100, handler)
	for i := 0; i < goroutineCount; i++ {
		go service.Publish(sampleEvent{})
	}
	it.WaitEventually(t, wg)
	service.Stop()
	if int32(goroutineCount) != dispatchCount {
		t.Fatalf("target %d != %d", goroutineCount, dispatchCount)
	}
}

func TestDispatchServiceUnsubscribe(t *testing.T) {
	service := event.NewDispatchService(logger.New())
	defer service.Stop()
	dispatchCount := int32(0)
	handler := func(event event.Event) {
		atomic.AddInt32(&dispatchCount, 1)
	}
	service.Subscribe("sample.event", 100, handler)
	service.Unsubscribe("sample.event", 100)
	service.Publish(sampleEvent{})
	it.Never(t, func() bool {
		return atomic.LoadInt32(&dispatchCount) != 0
	})
}

func TestDispatchServiceStop(t *testing.T) {
	service := event.NewDispatchService(logger.New())
	dispatchCount := int32(0)
	handler := func(event event.Event) {
		atomic.AddInt32(&dispatchCount, 1)
	}
	service.Subscribe("sample.event", 100, handler)
	service.Stop()
	assert.False(t, service.Publish(sampleEvent{}))
	it.Never(t, func() bool {
		return atomic.LoadInt32(&dispatchCount) != 0
	})
	service.Stop()
}

func TestDispatchServiceOrderIsGuaranteed(t *testing.T) {
	// the order of events should be guaranteed when using subscribe sync
	lg := logger.New()
	service := event.NewDispatchService(lg)
	wg := &sync.WaitGroup{}
	const targetCount = 1000
	wg.Add(targetCount)
	var values []int
	valuesMu := &sync.Mutex{}
	handler := func(event event.Event) {
		valuesMu.Lock()
		values = append(values, event.(sampleEvent).value)
		valuesMu.Unlock()
		wg.Done()
	}
	service.Subscribe("sample.event", 100, handler)
	for i := 0; i < targetCount; i++ {
		service.Publish(sampleEvent{value: i})
	}
	it.WaitEventually(t, wg)
	target := make([]int, targetCount)
	for i := 0; i < targetCount; i++ {
		target[i] = i
	}
	assert.Equal(t, target, values)
}

func TestDispatchServiceAllPublishedAreHandledBeforeClose(t *testing.T) {
	goroutineCount := 10000
	dispatchCount := int32(0)
	handler := func(event event.Event) {
		atomic.AddInt32(&dispatchCount, 1)
	}
	service := event.NewDispatchService(logger.New())
	service.Subscribe("sample.event", 100, handler)
	go service.Stop()
	successfulPubCnt := int32(0)
	for i := 0; i < goroutineCount; i++ {
		if service.Publish(sampleEvent{}) {
			successfulPubCnt++
		}
	}
	it.Eventually(t, func() bool {
		return successfulPubCnt == atomic.LoadInt32(&dispatchCount)
	})
	it.Never(t, func() bool {
		return successfulPubCnt != atomic.LoadInt32(&dispatchCount)
	})
}
