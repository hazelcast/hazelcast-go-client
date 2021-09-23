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
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

const DefaultSubscriptionID = -1

type Event interface {
	EventName() string
}

const (
	ready int32 = iota
	stopped
)

type Handler func(event Event)

type DispatchService struct {
	logger             logger.Logger
	subscriptions      map[string]map[int64]*subscription
	subscriptionsMutex *sync.RWMutex
	state              int32
}

// NewDispatchService creates a dispatch service with the following properties.
//1- It is async for the caller of Publish. Meaning Publish will not be blocked.
//It can be blocked because we don't have infinite channels. The queue size is 1024.
//2- Events fired from the same thread for the same subscription will be handled in the same order.
//One will finish, then other will start.
//3 - If we block an event, it will not block all events, only ones that are related to same subscription.
//4 - A close after publish in the same thread waits for published item to be handled(finished) .
func NewDispatchService(logger logger.Logger) *DispatchService {
	service := &DispatchService{
		subscriptions:      map[string]map[int64]*subscription{},
		subscriptionsMutex: &sync.RWMutex{},
		logger:             logger,
		state:              ready,
	}
	return service
}

// Stop the DispatchService and make sure that all the successful Publish events are handled before close
func (s *DispatchService) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&s.state, ready, stopped) {
		return nil
	}

	done := make(chan struct{})
	go func() {
		s.subscriptionsMutex.RLock()
		defer s.subscriptionsMutex.RUnlock()
		for _, eventSubscriptions := range s.subscriptions {
			for _, sbs := range eventSubscriptions {
				sbs.Stop()
			}
		}
		done <- struct{}{}
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shutting down dispatch service: %w", ctx.Err())
	}
}

// Subscribe attaches handler to listen for events with eventName.
// Do not rely on the order of handlers, they may be shuffled.
func (s *DispatchService) Subscribe(eventName string, subscriptionID int64, handler Handler) {
	if subscriptionID == DefaultSubscriptionID {
		subscriptionID = MakeSubscriptionID(handler)
	}
	s.subscriptionsMutex.Lock()
	defer s.subscriptionsMutex.Unlock()
	// subscribing to a not-running service is no-op
	if atomic.LoadInt32(&s.state) == stopped {
		return
	}

	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Subscribe: %s, %d, %v", eventName, subscriptionID, handler)
	})

	sbs := NewSubscription(handler)

	subscriptionHandlers, ok := s.subscriptions[eventName]
	if !ok {
		subscriptionHandlers = map[int64]*subscription{}
		s.subscriptions[eventName] = subscriptionHandlers
	}
	_, subExists := subscriptionHandlers[subscriptionID]
	if subExists {
		//TODO we need to make sure that this can never happen
		panic("subscriptionID already exists ")
	}
	subscriptionHandlers[subscriptionID] = sbs
}

func (s *DispatchService) Unsubscribe(eventName string, subscriptionID int64) {
	s.subscriptionsMutex.RLock()
	// unsubscribing from a not-running service is no-op
	if atomic.LoadInt32(&s.state) == stopped {
		s.subscriptionsMutex.RUnlock()
		return
	}
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Unsubscribe: %s, %d", eventName, subscriptionID)
	})
	if eventSubscriptions, ok := s.subscriptions[eventName]; ok {
		sbs, exists := eventSubscriptions[subscriptionID]
		if exists {
			sbs.Stop()
		}
	}
	s.subscriptionsMutex.RUnlock()

	s.subscriptionsMutex.Lock()
	defer s.subscriptionsMutex.Unlock()
	delete(s.subscriptions[eventName], subscriptionID)
}

// Publish an event. Events with the subscription are guaranteed to be run on the same order.
// If this method returns true, it is guaranteed that the dispatch service close wait for all events to be handled.
// Returns false if Dispatch Service is already closed.
func (s *DispatchService) Publish(event Event) bool {
	s.subscriptionsMutex.RLock()
	defer s.subscriptionsMutex.RUnlock()

	if atomic.LoadInt32(&s.state) == stopped {
		return false
	}
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Publish: %s", event.EventName())
	})
	if subs, ok := s.subscriptions[event.EventName()]; ok {
		for _, sub := range subs {
			sub.Publish(event)
		}
	}
	return true
}

type subscription struct {
	handler   Handler
	orderChan chan Event
	mutex     *sync.RWMutex
	closedWg  *sync.WaitGroup
	isClosed  bool
}

func NewSubscription(handler Handler) *subscription {
	sbs := subscription{handler: handler, orderChan: make(chan Event, 1024),
		closedWg: &sync.WaitGroup{}, mutex: &sync.RWMutex{}, isClosed: false}
	sbs.closedWg.Add(1)

	go func() {
		for event := range sbs.orderChan {
			sbs.handler(event)
		}
		sbs.closedWg.Done()
	}()
	return &sbs
}

// Stop the subscription
// - makes sure that Publishes will return false after this point.
// - makes sure that all successful Publishes will end before returning
// It is illegal to Stop a subscription inside its own handler( DEADLOCK )
func (s *subscription) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.isClosed {
		return
	}
	s.isClosed = true

	close(s.orderChan)
	s.closedWg.Wait()
}

// Publish an event to the subscription.
//If it returns true,
//it is guaranteed that event will run before the subscription is stopped
func (s *subscription) Publish(event Event) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.isClosed {
		return false
	}

	s.orderChan <- event
	return true
}
