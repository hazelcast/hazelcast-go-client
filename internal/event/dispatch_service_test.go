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
	lg := logger.New()
	service := event.NewDispatchService(lg)
	service.Subscribe("sample.event", 100, handler)
	for i := 0; i < goroutineCount; i++ {
		go service.Publish(sampleEvent{})
	}
	wg.Wait()
	service.Stop()
	if int32(goroutineCount) != dispatchCount {
		t.Fatalf("target %d != %d", goroutineCount, dispatchCount)
	}
}

func TestDispatchServiceUnsubscribe(t *testing.T) {
	lg := logger.New()
	service := event.NewDispatchService(lg)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	dispatchCount := int32(0)
	handler := func(event event.Event) {
		atomic.AddInt32(&dispatchCount, 1)
		wg.Done()
	}
	service.Subscribe("sample.event", 100, handler)
	service.Publish(sampleEvent{})
	wg.Wait()
	service.Unsubscribe("sample.event", 100)
	service.Publish(sampleEvent{})
	wg.Wait()
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
