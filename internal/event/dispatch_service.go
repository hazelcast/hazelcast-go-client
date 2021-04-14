package event

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const DefaultSubscriptionID = -1

type Event interface {
	EventName() string
}

type EventHandler func(event Event)

type controlType int

const (
	subscribe controlType = iota
	subscribeSync
	unsubscribe
)

type controlMessage struct {
	controlType    controlType
	eventName      string
	subscriptionID int
	handler        EventHandler
}

type DispatchService struct {
	subscriptions     map[string]map[int]EventHandler
	syncSubscriptions map[string]map[int]EventHandler
	eventCh           chan Event
	controlCh         chan controlMessage
	running           atomic.Value
}

func NewDispatchService() *DispatchService {
	service := &DispatchService{
		subscriptions:     map[string]map[int]EventHandler{},
		syncSubscriptions: map[string]map[int]EventHandler{},
		eventCh:           make(chan Event, 1),
		controlCh:         make(chan controlMessage, 1),
	}
	service.running.Store(false)
	service.start()
	return service
}

// Subscribe attaches handler to listen for events with eventName.
// Do not rely on the order of handlers, they may be shuffled.
func (s *DispatchService) Subscribe(eventName string, subscriptionID int, handler EventHandler) {
	// subscribing to a not-runnning service is no-op
	if s.running.Load() != true {
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

// Subscribe attaches handler to listen for events with eventName.
// Do not rely on the order of handlers, they may be shuffled.
func (s *DispatchService) SubscribeSync(eventName string, subscriptionID int, handler EventHandler) {
	// subscribing to a not-runnning service is no-op
	if s.running.Load() != true {
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

func (s *DispatchService) Unsubscribe(eventName string, subscriptionID int) {
	// unsubscribing from a not-runnning service is no-op
	if s.running.Load() != true {
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
	if s.running.Load() != true || event == nil {
		return
	}
	s.eventCh <- event
}

func (s *DispatchService) start() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Done()
		for {
			select {
			case event, ok := <-s.eventCh:
				if ok {
					s.dispatch(event)
				}
			case control, ok := <-s.controlCh:
				if !ok {
					continue
				}
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
			}
		}
	}()
	wg.Wait()
	s.running.Store(true)
}

func (s *DispatchService) Stop() {
	// stopping a not-running service is no-op
	if s.running.Load() != true {
		return
	}
	s.running.Store(false)
	//s.doneCh <- struct{}{}
	//close(s.doneCh)
	close(s.eventCh)
	close(s.controlCh)
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

func (s *DispatchService) subscribe(eventName string, subscriptionID int, handler EventHandler) {
	subscriptionHandlers, ok := s.subscriptions[eventName]
	if !ok {
		subscriptionHandlers = map[int]EventHandler{}
		s.subscriptions[eventName] = subscriptionHandlers
	}
	subscriptionHandlers[subscriptionID] = handler
}

func (s *DispatchService) subscribeSync(eventName string, subscriptionID int, handler EventHandler) {
	subscriptionHandlers, ok := s.syncSubscriptions[eventName]
	if !ok {
		subscriptionHandlers = map[int]EventHandler{}
		s.syncSubscriptions[eventName] = subscriptionHandlers
	}
	subscriptionHandlers[subscriptionID] = handler
}

func (s *DispatchService) unsubscribe(eventName string, unsubscribeSubscriptionID int) {
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
