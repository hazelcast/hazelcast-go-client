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

package event

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
)

var globalSubscriptionID = int64(0)

func NextSubscriptionID() int64 {
	return atomic.AddInt64(&globalSubscriptionID, 1)
}

type Event interface {
	EventName() string
}

const (
	ready int32 = iota
	stopped
)

type Handler func(event Event)

type DispatchService struct {
	logger          ilogger.LogAdaptor
	subscriptions   map[string]map[int64]*subscription
	subscriptionsMu *sync.RWMutex
	state           int32
}

// NewDispatchService creates a dispatch service with the following properties.
//1- It is async for the caller of Publish. Meaning Publish will not be blocked.
//It can be blocked because we don't have infinite channels. The queue size is 1024.
//2- Events fired from the same thread for the same subscription will be handled in the same order.
//One will finish, then other will start.
//3 - If we block an event, it will not block all events, only ones that are related to same subscription.
//4 - A close after publish in the same thread waits for published item to be handled(finished) .
func NewDispatchService(logger ilogger.LogAdaptor) *DispatchService {
	service := &DispatchService{
		subscriptions:   map[string]map[int64]*subscription{},
		subscriptionsMu: &sync.RWMutex{},
		logger:          logger,
		state:           ready,
	}
	return service
}

// Stop the DispatchService and make sure that all the successful Publish events are handled before close
func (s *DispatchService) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&s.state, ready, stopped) {
		return nil
	}

	doneCh := make(chan struct{})
	go func() {
		s.subscriptionsMu.RLock()
		defer s.subscriptionsMu.RUnlock()
		for _, eventSubscriptions := range s.subscriptions {
			for _, sbs := range eventSubscriptions {
				sbs.Stop()
			}
		}
		close(doneCh)
	}()
	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shutting down dispatch service: %w", ctx.Err())
	}
}

// Subscribe attaches handler to listen for events with eventName.
// Do not rely on the order of handlers, they may be shuffled.
func (s *DispatchService) Subscribe(eventName string, subscriptionID int64, handler Handler) {
	// subscribing to a not-running service is no-op
	if atomic.LoadInt32(&s.state) == stopped {
		return
	}

	s.subscriptionsMu.Lock()
	defer s.subscriptionsMu.Unlock()

	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Subscribe: %s, %d, %v", eventName, subscriptionID, handler)
	})

	sbs := NewSubscription(handler)

	handlers, ok := s.subscriptions[eventName]
	if !ok {
		handlers = map[int64]*subscription{}
		s.subscriptions[eventName] = handlers
	}
	if _, exists := handlers[subscriptionID]; exists {
		//TODO we need to make sure that this can never happen
		panic("subscriptionID already exists ")
	}
	handlers[subscriptionID] = sbs
}

func (s *DispatchService) Unsubscribe(eventName string, subscriptionID int64) {
	// unsubscribing from a not-running service is no-op
	if atomic.LoadInt32(&s.state) == stopped {
		return
	}
	s.subscriptionsMu.RLock()
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Unsubscribe: %s, %d", eventName, subscriptionID)
	})
	if subs, ok := s.subscriptions[eventName]; ok {
		if sub, exists := subs[subscriptionID]; exists {
			sub.Stop()
		}
	}
	s.subscriptionsMu.RUnlock()

	s.subscriptionsMu.Lock()
	delete(s.subscriptions[eventName], subscriptionID)
	s.subscriptionsMu.Unlock()
}

// Publish an event. Events with the subscription are guaranteed to be run on the same order.
// If this method returns true, it is guaranteed that the dispatch service close wait for all events to be handled.
// Returns false if Dispatch Service is already closed.
func (s *DispatchService) Publish(event Event) bool {
	if atomic.LoadInt32(&s.state) == stopped {
		return false
	}

	s.subscriptionsMu.RLock()
	defer s.subscriptionsMu.RUnlock()
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Publish: %s", event.EventName())
	})
	if subs, ok := s.subscriptions[event.EventName()]; ok {
		for _, sub := range subs {
			if okp := sub.Publish(event); !okp {
				return false
			}
		}
	}
	return true
}

type subscription struct {
	handler  Handler
	orderCh  chan Event
	mu       *sync.RWMutex
	closedWg *sync.WaitGroup
	closed   bool
}

func NewSubscription(handler Handler) *subscription {
	sbs := subscription{
		handler:  handler,
		orderCh:  make(chan Event, 1024),
		closedWg: &sync.WaitGroup{},
		mu:       &sync.RWMutex{},
		closed:   false,
	}
	sbs.closedWg.Add(1)

	go func() {
		for event := range sbs.orderCh {
			sbs.handler(event)
		}
		sbs.closedWg.Done()
	}()
	return &sbs
}

//Stop ends the subscription.
//It makes sure that any Publish will return false and all successful publishes will end before returning.
//Stop must not be called inside its own handler, which can cause a deadlock.
func (s *subscription) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true

	close(s.orderCh)
	s.closedWg.Wait()
}

// Publish publishes an event to the subscription.
// It is guaranteed that event will run before the subscription is stopped if it returns true.
func (s *subscription) Publish(event Event) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false
	}

	s.orderCh <- event
	return true
}
