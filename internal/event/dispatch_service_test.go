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

package event_test

import (
	"context"
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

type differentEvent struct {
	value int
}

func (e differentEvent) EventName() string {
	return "different.event"
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
	service := event.NewDispatchService(logger.LogAdaptor{Logger: logger.New()})
	service.Subscribe("sample.event", 100, handler)
	for i := 0; i < goroutineCount; i++ {
		go service.Publish(sampleEvent{})
	}
	it.WaitEventually(t, wg)
	service.Stop(context.Background())
	if int32(goroutineCount) != dispatchCount {
		t.Fatalf("target %d != %d", goroutineCount, dispatchCount)
	}
}

func TestDispatchServiceUnsubscribe(t *testing.T) {
	service := event.NewDispatchService(logger.LogAdaptor{Logger: logger.New()})
	defer service.Stop(context.Background())
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
	service := event.NewDispatchService(logger.LogAdaptor{Logger: logger.New()})
	dispatchCount := int32(0)
	handler := func(event event.Event) {
		atomic.AddInt32(&dispatchCount, 1)
	}
	service.Subscribe("sample.event", 100, handler)
	service.Stop(context.Background())
	assert.False(t, service.Publish(sampleEvent{}))
	it.Never(t, func() bool {
		return atomic.LoadInt32(&dispatchCount) != 0
	})
	service.Stop(context.Background())
}

func TestDispatchServiceOrderIsGuaranteed(t *testing.T) {
	// the order of events should be guaranteed when using subscribe sync
	service := event.NewDispatchService(logger.LogAdaptor{Logger: logger.New()})
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
	goroutineCount := 10_000
	dispatchCount := int32(0)
	handler := func(event event.Event) {
		atomic.AddInt32(&dispatchCount, 1)
	}
	service := event.NewDispatchService(logger.LogAdaptor{Logger: logger.New()})
	service.Subscribe("sample.event", 100, handler)
	go service.Stop(context.Background())
	successfulPubCnt := int32(0)
	for i := 0; i < goroutineCount; i++ {
		if service.Publish(sampleEvent{}) {
			atomic.AddInt32(&successfulPubCnt, 1)
		}
	}
	it.Eventually(t, func() bool {
		success := atomic.LoadInt32(&successfulPubCnt)
		dispatch := atomic.LoadInt32(&dispatchCount)
		t.Logf("pub cnt: dispatched: %d, success: %d", dispatch, success)
		return success == dispatch
	})
	it.Never(t, func() bool {
		return atomic.LoadInt32(&successfulPubCnt) != atomic.LoadInt32(&dispatchCount)
	})
}

func TestDispatchService_BlockingCallWillNotBlockUnrelatedSubscriptions(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()
	service := event.NewDispatchService(logger.LogAdaptor{Logger: logger.New()})
	service.Subscribe("sample.event", 1, func(event event.Event) {
		//Wait blocking until test finishes.
		wg.Wait()
	})
	sameNameWg := &sync.WaitGroup{}
	sameNameWg.Add(1)
	diffNameWg := &sync.WaitGroup{}
	diffNameWg.Add(1)
	service.Subscribe("sample.event", 2, func(event event.Event) {
		sameNameWg.Done()
	})
	service.Subscribe("different.event", 3, func(event event.Event) {
		diffNameWg.Done()
	})
	service.Publish(sampleEvent{1})
	service.Publish(differentEvent{1})
	it.WaitEventually(t, sameNameWg)
	it.WaitEventually(t, diffNameWg)
}

func TestDispatchServiceCloseRespectsContext(t *testing.T) {
	service := event.NewDispatchService(logger.LogAdaptor{Logger: logger.New()})
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	handler := func(event event.Event) {
		wg.Wait()
	}
	service.Subscribe("sample.event", 100, handler)
	service.Publish(sampleEvent{})
	cancel()
	err := service.Stop(ctx)
	assert.NotNil(t, err)
	wg.Done()
}
