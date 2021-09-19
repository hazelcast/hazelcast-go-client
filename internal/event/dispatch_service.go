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

package event

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

// DefaultSubscriptionID enables automatically setting a subscription ID.
const DefaultSubscriptionID = -1

// Event is a value published by an event publishers and delivered to one or more subscribbers via the dispatch service.
type Event interface {
	EventName() string
}

// SyncHandler is the type for synchronous callbacks.
type SyncHandler func(event Event)

// AsyncHandler is the type for asynchronous callbacks.
type AsyncHandler func(event Event, wg *sync.WaitGroup)

const (
	ready   = 0
	stopped = 1
)

type subscriptionMap map[string]*sync.Map

// DispatchService delivers events from a publisher to one or more subscribers.
type DispatchService struct {
	logger             logger.Logger
	asyncSubscriptions atomic.Value
	syncSubscriptions  atomic.Value
	subMu              *sync.Mutex
	state              int32
}

// NewDispatchService creates a dispatch service with the given logger.
func NewDispatchService(logger logger.Logger) *DispatchService {
	service := &DispatchService{
		subMu:  &sync.Mutex{},
		logger: logger,
	}
	service.asyncSubscriptions.Store(subscriptionMap{})
	service.syncSubscriptions.Store(subscriptionMap{})
	return service
}

// Stop terminates the dispatch service and releases the resources.
// It is not possible to subscribe to or publish events once the dispatch service is terminated.
// Calling Stop more than once is no-op.
func (s *DispatchService) Stop() {
	// stopping a not-running service is no-op
	if !atomic.CompareAndSwapInt32(&s.state, ready, stopped) {
		return
	}
}

// Subscribe attaches a handler to listen for events with the given event name.
// Do not rely on the subscription order of handlers, they may be called back in a random order.
// The handler will be called in a goroutine, and the dispatch service makes sure that the corresponding goroutine is started before the goroutine for the next handler.
// Once the goroutines for handlers start, it's up to the runtime to schedule them.
func (s *DispatchService) Subscribe(eventName string, subscriptionID int64, handler SyncHandler) {
	// subscribing to a not-running service is no-op
	if atomic.LoadInt32(&s.state) != ready {
		return
	}
	if subscriptionID == DefaultSubscriptionID {
		subscriptionID = MakeSubscriptionID(handler)
	}
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Subscribe: %s, %d, %v", eventName, subscriptionID, handler)
	})
	s.subscribeAsync(eventName, subscriptionID, func(event Event, wg *sync.WaitGroup) {
		wg.Done()
		handler(event)
	})
}

// SubscribeSync attaches handler to listen for events with the given event name.
// Sync handlers are dispatched first, the events are ordered.
// Do not rely on the subscription order of handlers, they may be called back in a random order.
// The handler is called in the same goroutine with the dispatch service and executes until the end of the corresponding function.
// Make sure sync handlers do no block.
func (s *DispatchService) SubscribeSync(eventName string, subscriptionID int64, handler SyncHandler) {
	// subscribing to a not-runnning service is no-op
	if atomic.LoadInt32(&s.state) != ready {
		return
	}
	if subscriptionID == DefaultSubscriptionID {
		subscriptionID = MakeSubscriptionID(handler)
	}
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.SubscribeSync: %s, %d, %v", eventName, subscriptionID, handler)
	})
	s.subscribeSync(eventName, subscriptionID, handler)
}

// Unsubscribe detaches the handler with the given subscription ID from the given event name.
// Detached handlers will not be called back during the dispatch for the corresponding event.
// Detaching the handler has no effect on an already started handler.
func (s *DispatchService) Unsubscribe(eventName string, subscriptionID int64) {
	// unsubscribing from a not-running service is no-op
	if atomic.LoadInt32(&s.state) != ready {
		return
	}
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Unsubscribe: %s, %d", eventName, subscriptionID)
	})
	s.unsubscribe(eventName, subscriptionID)
}

// Publish delivers the given event to subscribers.
// The event will first be delivered to sync subscribers, then the other ones.
func (s *DispatchService) Publish(event Event) {
	// publishing to a not-running service is no-op
	if atomic.LoadInt32(&s.state) != ready {
		return
	}
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Publish: %s", event.EventName())
	})
	s.dispatch(event)
}

func (s *DispatchService) dispatch(event Event) {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.dispatch: %s", event.EventName())
	})
	// first dispatch sync handlers
	if handlers, ok := s.loadSyncSubs()[event.EventName()]; ok {
		handlers.Range(func(k, v interface{}) bool {
			v.(SyncHandler)(event)
			return true
		})
	}
	// then dispatch async handlers
	wg := &sync.WaitGroup{}
	if handlers, ok := s.loadAsyncSubs()[event.EventName()]; ok {
		handlers.Range(func(k, v interface{}) bool {
			wg.Add(1)
			go v.(AsyncHandler)(event, wg)
			wg.Wait()
			return true
		})
	}
}

func (s *DispatchService) subscribeAsync(eventName string, subscriptionID int64, handler AsyncHandler) {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.subscribeAsync: %s, %d", eventName, subscriptionID)
	})
	s.subMu.Lock()
	defer s.subMu.Unlock()
	subs := s.loadAsyncSubs()
	handlers, ok := subs[eventName]
	if !ok {
		handlers = &sync.Map{}
		subs[eventName] = handlers
	}
	handlers.Store(subscriptionID, handler)
	s.asyncSubscriptions.Store(subs)
}

func (s *DispatchService) subscribeSync(eventName string, subscriptionID int64, handler SyncHandler) {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.subscribeSync: %s, %d", eventName, subscriptionID)
	})
	s.subMu.Lock()
	defer s.subMu.Unlock()
	subs := s.loadSyncSubs()
	handlers, ok := subs[eventName]
	if !ok {
		handlers = &sync.Map{}
		subs[eventName] = handlers
	}
	handlers.Store(subscriptionID, handler)
	s.syncSubscriptions.Store(subs)
}

func (s *DispatchService) unsubscribe(eventName string, subscriptionID int64) {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.unsubscribe: %s, %d", eventName, subscriptionID)
	})
	s.subMu.Lock()
	defer s.subMu.Unlock()
	s.unsubscribeSync(eventName, subscriptionID)
	s.unsubscribeAsync(eventName, subscriptionID)
}

func (s *DispatchService) unsubscribeSync(eventName string, subscriptionID int64) {
	syncs := s.loadSyncSubs()
	if handlers, ok := syncs[eventName]; ok {
		handlers.Range(func(sid, v interface{}) bool {
			if sid == subscriptionID {
				handlers.Delete(sid)
				s.syncSubscriptions.Store(syncs)
				return false
			}
			return true
		})
	}
}

func (s *DispatchService) unsubscribeAsync(eventName string, subscriptionID int64) {
	asyncs := s.loadAsyncSubs()
	if handlers, ok := asyncs[eventName]; ok {
		handlers.Range(func(sid, v interface{}) bool {
			if sid == subscriptionID {
				handlers.Delete(sid)
				s.asyncSubscriptions.Store(asyncs)
				return false
			}
			return true
		})
	}
}

func (s *DispatchService) loadSyncSubs() subscriptionMap {
	return s.syncSubscriptions.Load().(subscriptionMap)
}

func (s *DispatchService) loadAsyncSubs() subscriptionMap {
	return s.asyncSubscriptions.Load().(subscriptionMap)
}
