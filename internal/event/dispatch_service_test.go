package event_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
)

type sampleEvent struct {
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
	service := event.NewDispatchService()
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
	service := event.NewDispatchService()
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
	time.Sleep(100 * time.Millisecond)
	service.Stop()
	if int32(1) != dispatchCount {
		t.Fatalf("target 1 != %d", dispatchCount)
	}
}
