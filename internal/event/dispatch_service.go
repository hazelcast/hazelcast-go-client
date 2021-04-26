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
	"sync/atomic"
)

const DefaultSubscriptionID = -1

type Event interface {
	EventName() string
}

type Handler func(event Event)

type controlType int

const (
	subscribe controlType = iota
	subscribeSync
	unsubscribe
)

const (
	created int32 = iota
	ready
	stopped
)

type controlMessage struct {
	controlType    controlType
	eventName      string
	subscriptionID int64
	handler        Handler
}

type DispatchService struct {
	subscriptions     map[string]map[int64]Handler
	syncSubscriptions map[string]map[int64]Handler
	eventCh           chan Event
	controlCh         chan controlMessage
	doneCh            chan struct{}
	state             int32
}

func NewDispatchService() *DispatchService {
	service := &DispatchService{
		subscriptions:     map[string]map[int64]Handler{},
		syncSubscriptions: map[string]map[int64]Handler{},
		eventCh:           make(chan Event, 1),
		controlCh:         make(chan controlMessage, 1),
		doneCh:            make(chan struct{}),
		state:             created,
	}
	startCh := make(chan struct{})
	go service.start(startCh)
	<-startCh
	atomic.StoreInt32(&service.state, ready)
	return service
}

func (s *DispatchService) Stop() {
	// stopping a not-running service is no-op
	if !atomic.CompareAndSwapInt32(&s.state, ready, stopped) {
		return
	}
	close(s.doneCh)
}

// Subscribe attaches handler to listen for events with eventName.
// Do not rely on the order of handlers, they may be shuffled.
func (s *DispatchService) Subscribe(eventName string, subscriptionID int64, handler Handler) {
	// subscribing to a not-runnning service is no-op
	if atomic.LoadInt32(&s.state) != ready {
		return
	}
	if subscriptionID == DefaultSubscriptionID {
		subscriptionID = MakeSubscriptionID(handler)
	}
	s.controlCh <- controlMessage{
		controlType:    subscribe,
		eventName:      eventName,
		subscriptionID: subscriptionID,
		handler:        handler,
	}
}

// SubscribeSync attaches handler to listen for events with eventName.
// Sync handlers are dispatched first, the events are ordered.
// Do not rely on the order of handlers, they may be shuffled.
func (s *DispatchService) SubscribeSync(eventName string, subscriptionID int64, handler Handler) {
	// subscribing to a not-runnning service is no-op
	if atomic.LoadInt32(&s.state) != ready {
		return
	}
	if subscriptionID == DefaultSubscriptionID {
		subscriptionID = MakeSubscriptionID(handler)
	}
	s.controlCh <- controlMessage{
		controlType:    subscribeSync,
		eventName:      eventName,
		subscriptionID: subscriptionID,
		handler:        handler,
	}
}

func (s *DispatchService) Unsubscribe(eventName string, subscriptionID int64) {
	// unsubscribing from a not-runnning service is no-op
	if atomic.LoadInt32(&s.state) != ready {
		return
	}
	s.controlCh <- controlMessage{
		// TODO: rename controlType
		controlType:    unsubscribe,
		eventName:      eventName,
		subscriptionID: subscriptionID,
	}
}

func (s *DispatchService) Publish(event Event) {
	// publishing to a not-runnning service is no-op
	if atomic.LoadInt32(&s.state) != ready {
		return
	}
	s.eventCh <- event
}

func (s *DispatchService) start(startCh chan<- struct{}) {
	startCh <- struct{}{}
	for {
		select {
		case event := <-s.eventCh:
			s.dispatch(event)
		case control := <-s.controlCh:
			switch control.controlType {
			case subscribe:
				s.subscribe(control.eventName, control.subscriptionID, control.handler)
			case subscribeSync:
				s.subscribeSync(control.eventName, control.subscriptionID, control.handler)
			case unsubscribe:
				s.unsubscribe(control.eventName, control.subscriptionID)
			default:
				panic(fmt.Sprintf("unknown control type: %d", control.controlType))
			}
		case <-s.doneCh:
			return
		}
	}
}

func (s *DispatchService) dispatch(event Event) {
	// first dispatch sync handlers
	if handlers, ok := s.syncSubscriptions[event.EventName()]; ok {
		for _, handler := range handlers {
			handler(event)
		}
	}
	// then dispatch async handlers
	if handlers, ok := s.subscriptions[event.EventName()]; ok {
		for _, handler := range handlers {
			go handler(event)
		}
	}
}

func (s *DispatchService) subscribe(eventName string, subscriptionID int64, handler Handler) {
	subscriptionHandlers, ok := s.subscriptions[eventName]
	if !ok {
		subscriptionHandlers = map[int64]Handler{}
		s.subscriptions[eventName] = subscriptionHandlers
	}
	subscriptionHandlers[subscriptionID] = handler
}

func (s *DispatchService) subscribeSync(eventName string, subscriptionID int64, handler Handler) {
	subscriptionHandlers, ok := s.syncSubscriptions[eventName]
	if !ok {
		subscriptionHandlers = map[int64]Handler{}
		s.syncSubscriptions[eventName] = subscriptionHandlers
	}
	subscriptionHandlers[subscriptionID] = handler
}

func (s *DispatchService) unsubscribe(eventName string, unsubscribeSubscriptionID int64) {
	if handlers, ok := s.syncSubscriptions[eventName]; ok {
		for subscriptionID, _ := range handlers {
			if subscriptionID == unsubscribeSubscriptionID {
				delete(handlers, subscriptionID)
				return
			}
		}
	}
	if handlers, ok := s.subscriptions[eventName]; ok {
		for subscriptionID, _ := range handlers {
			if subscriptionID == unsubscribeSubscriptionID {
				delete(handlers, subscriptionID)
				break
			}
		}
	}
}
