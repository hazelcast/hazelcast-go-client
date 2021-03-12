package event_test

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type sampleEvent struct {
}

func (e sampleEvent) Name() string {
	return "sample.event"
}

func (e sampleEvent) Payload() interface{} {
	panic("implement me")
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
	service := event.NewDispatchServiceImpl()
	service.Subscribe("sample.event", handler)
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
	service := event.NewDispatchServiceImpl()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	dispatchCount := int32(0)
	handler := func(event event.Event) {
		atomic.AddInt32(&dispatchCount, 1)
		wg.Done()
	}
	service.Subscribe("sample.event", handler)
	service.Publish(sampleEvent{})
	wg.Wait()
	service.Unsubscribe("sample.event", handler)
	service.Publish(sampleEvent{})
	wg.Wait()
	time.Sleep(100 * time.Millisecond)
	service.Stop()
	if int32(1) != dispatchCount {
		t.Fatalf("target 1 != %d", dispatchCount)
	}
}
