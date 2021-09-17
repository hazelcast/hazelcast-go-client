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
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"sync"
)

const DefaultSubscriptionID = -1

type Event interface {
	EventName() string
}

type Handler func(event Event)

type subscription struct {
	handler   Handler
	orderChan chan Event
}

type DispatchService struct {
	logger        logger.Logger
	subscriptions map[string]map[int64]subscription
	eventMu       *sync.RWMutex
	isClosed      bool
}

func NewDispatchService(logger logger.Logger) *DispatchService {
	service := &DispatchService{
		subscriptions: map[string]map[int64]subscription{},
		eventMu:       &sync.RWMutex{},
		logger:        logger,
	}
	return service
}

func (s *DispatchService) Stop() {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	// stopping to a not-running service is no-op
	if s.isClosed {
		return
	}
	s.isClosed = true
	for _, eventSubscriptions := range s.subscriptions {
		for _, sbs := range eventSubscriptions {
			close(sbs.orderChan)
		}
	}
}

// Subscribe attaches handler to listen for events with eventName.
// Do not rely on the order of handlers, they may be shuffled.
func (s *DispatchService) Subscribe(eventName string, subscriptionID int64, handler Handler) {
	if subscriptionID == DefaultSubscriptionID {
		subscriptionID = MakeSubscriptionID(handler)
	}
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Subscribe: %s, %d, %v", eventName, subscriptionID, handler)
	})
	s.eventMu.Lock()
	defer s.eventMu.Unlock()

	// subscribing to a not-running service is no-op
	if s.isClosed {
		return
	}
	s.subscribe(eventName, subscriptionID, subscription{handler: handler, orderChan: make(chan Event, 1024)})
}

func (s *DispatchService) Unsubscribe(eventName string, subscriptionID int64) {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Unsubscribe: %s, %d", eventName, subscriptionID)
	})
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	// unsubscribing from a not-running service is no-op
	if s.isClosed {
		return
	}
	s.unsubscribe(eventName, subscriptionID)
}

func (s *DispatchService) Publish(event Event) bool {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Publish: %s", event.EventName())
	})
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	// publishing to a not-running service is no-op

	if s.isClosed {
		return false
	}
	return s.dispatch(event)
}

func (s *DispatchService) dispatch(event Event) bool {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.dispatch: %s", event.EventName())
	})
	if handlers, ok := s.subscriptions[event.EventName()]; ok {
		for _, handler := range handlers {
			handler.orderChan <- event
		}
		return true
	} else {
		return false
	}
}

func (s *DispatchService) subscribe(eventName string, subscriptionID int64, sbs subscription) {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.subscribe: %s, %d", eventName, subscriptionID)
	})
	subscriptionHandlers, ok := s.subscriptions[eventName]
	if !ok {
		subscriptionHandlers = map[int64]subscription{}
		s.subscriptions[eventName] = subscriptionHandlers
	}
	subscriptionHandlers[subscriptionID] = sbs
	go func() {
		for event := range sbs.orderChan {
			sbs.handler(event)
		}
	}()
}

func (s *DispatchService) unsubscribe(eventName string, subscriptionID int64) {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.unsubscribe: %s, %d", eventName, subscriptionID)
	})
	if handlers, ok := s.subscriptions[eventName]; ok {
		for sid := range handlers {
			if sid == subscriptionID {
				delete(handlers, sid)
				break
			}
		}
	}
}
