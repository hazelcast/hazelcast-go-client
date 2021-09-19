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
	"fmt"
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
	id    int
}

func (e sampleEvent) EventName() string {
	if e.id == 0 {
		return "sample.event"
	}
	return fmt.Sprintf("sample.event-%d", e.id)
}

func TestDispatchServiceSubscribePublish(t *testing.T) {
	goroutineCount := 10000
	twg := &sync.WaitGroup{}
	twg.Add(goroutineCount)
	dispatchCount := int32(0)
	handler := func(event event.Event) {
		atomic.AddInt32(&dispatchCount, 1)
		twg.Done()
	}
	lg := logger.New()
	service := event.NewDispatchService(lg)
	service.Subscribe("sample.event", 100, handler)
	for i := 0; i < goroutineCount; i++ {
		go service.Publish(sampleEvent{})
	}
	twg.Wait()
	service.Stop()
	if int32(goroutineCount) != dispatchCount {
		t.Fatalf("target %d != %d", goroutineCount, dispatchCount)
	}
}

func TestDispatchServiceUnsubscribe(t *testing.T) {
	lg := logger.New()
	service := event.NewDispatchService(lg)
	twg := &sync.WaitGroup{}
	twg.Add(1)
	dispatchCount := int32(0)
	handler := func(event event.Event) {
		atomic.AddInt32(&dispatchCount, 1)
		twg.Done()
	}
	service.Subscribe("sample.event", 100, handler)
	service.Publish(sampleEvent{})
	it.WaitEventually(t, twg)
	service.Unsubscribe("sample.event", 100)
	service.Publish(sampleEvent{})
	it.WaitEventually(t, twg)
	service.Stop()
	if int32(1) != dispatchCount {
		t.Fatalf("target 1 != %d", dispatchCount)
	}
}

func TestDispatchService_SubscribeSync(t *testing.T) {
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
	service.SubscribeSync("sample.event", 100, handler)
	for i := 0; i < targetCount; i++ {
		service.Publish(sampleEvent{value: i})
	}
	wg.Wait()
	target := make([]int, targetCount)
	for i := 0; i < targetCount; i++ {
		target[i] = i
	}
	assert.Equal(t, target, values)
}

func TestDispatchService_Subscribe(t *testing.T) {
	// The order of events of different type should be guaranteed when using subscribe.
	// The toleration here is required for the test, since although the goroutine for a handler is guaranteed to start, it is not guaranteed to end before the next handler.
	const outOfOrderValuesToleration = 0.6 // %0.6, 6 differences in order per 1000 events
	lg := logger.New()
	service := event.NewDispatchService(lg)
	twg := &sync.WaitGroup{}
	const targetCount = 1000
	twg.Add(targetCount)
	var values []int
	valuesMu := &sync.Mutex{}
	handler := func(event event.Event) {
		valuesMu.Lock()
		values = append(values, event.(sampleEvent).value)
		valuesMu.Unlock()
		twg.Done()
	}
	for i := 1; i <= targetCount; i++ {
		service.Subscribe(fmt.Sprintf("sample.event-%d", i), int64(100+i), handler)
	}
	for i := 1; i <= targetCount; i++ {
		service.Publish(sampleEvent{value: i, id: i})
	}
	twg.Wait()
	target := make([]int, targetCount)
	for i := 1; i <= targetCount; i++ {
		target[i-1] = i
	}
	// tolerate some amount of out-of-order values
	diff := 0
	for i := 0; i < targetCount; i++ {
		if target[i] != values[i] {
			diff++
		}
	}
	rate := 100 * float64(diff) / targetCount
	if rate > outOfOrderValuesToleration {
		t.Fatalf("Out of order value rate: %f", rate)
	}
}
