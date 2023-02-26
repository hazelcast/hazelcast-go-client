/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package invocation

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

var (
	serviceSubID = event.NextSubscriptionID()
	stateSubID   = event.NextSubscriptionID()
)

type Handler interface {
	Invoke(invocation Invocation) (groupID int64, err error)
}

type Service struct {
	handler         Handler
	requestCh       chan Invocation
	responseCh      chan *proto.ClientMessage
	doneCh          chan struct{}
	groupLostCh     chan *GroupLostEvent
	invocations     map[int64]Invocation
	urgentRequestCh chan Invocation
	eventDispatcher *event.DispatchService
	// removeCh carries correlationIDs to be removed
	removeCh chan int64
	executor *stripeExecutor
	logger   logger.LogAdaptor
	stateMu  *sync.RWMutex
	running  bool
	paused   int32
}

func NewService(handler Handler, ed *event.DispatchService, lg logger.LogAdaptor) *Service {
	s := &Service{
		requestCh:       make(chan Invocation),
		urgentRequestCh: make(chan Invocation),
		responseCh:      make(chan *proto.ClientMessage),
		removeCh:        make(chan int64),
		doneCh:          make(chan struct{}),
		groupLostCh:     make(chan *GroupLostEvent),
		invocations:     map[int64]Invocation{},
		handler:         handler,
		eventDispatcher: ed,
		logger:          lg,
		stateMu:         &sync.RWMutex{},
		running:         true,
		executor:        newStripeExecutor(),
	}
	s.eventDispatcher.Subscribe(EventGroupLost, serviceSubID, func(event event.Event) {
		go func() {
			select {
			case s.groupLostCh <- event.(*GroupLostEvent):
				return
			case <-s.doneCh:
				return
			}
		}()
	})
	s.eventDispatcher.Subscribe(EventInvocationStateChanged, stateSubID, func(event event.Event) {
		e := event.(*InvocationStateChanged)
		if !e.Enabled {
			atomic.CompareAndSwapInt32(&s.paused, 0, 1)
			s.logger.Debug(func() string {
				return "invocation.Service: disabled non-urgent invocations"
			})
			return
		}
		atomic.CompareAndSwapInt32(&s.paused, 1, 0)
		s.logger.Debug(func() string {
			return "invocation.Service: enabled non-urgent invocations"
		})
	})
	s.executor.start()
	go s.processIncoming()
	return s
}

func (s *Service) Stop() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if !s.running {
		return
	}
	s.running = false
	s.executor.stop()
	close(s.doneCh)
}

func (s *Service) SetHandler(handler Handler) {
	s.handler = handler
}

func (s *Service) SendRequest(ctx context.Context, inv Invocation) error {
	if atomic.LoadInt32(&s.paused) == 1 {
		err := fmt.Errorf("non-urgent invocations are paused: %w", hzerrors.ErrRetryableIO)
		if time.Now().After(inv.Deadline()) {
			return cb.WrapNonRetryableError(err)
		}
		return err
	}
	select {
	case <-ctx.Done():
		return cb.WrapNonRetryableError(fmt.Errorf("sending invocation: %w", ctx.Err()))
	case <-s.doneCh:
		return cb.WrapNonRetryableError(fmt.Errorf("sending invocation: %w", hzerrors.ErrClientNotActive))
	case s.requestCh <- inv:
		return nil
	}
}

func (s *Service) SendUrgentRequest(ctx context.Context, inv Invocation) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("sending urgent invocation: %w", ctx.Err())
	case <-s.doneCh:
		return cb.WrapNonRetryableError(fmt.Errorf("sending urgent invocation: %w", hzerrors.ErrClientNotActive))
	case s.urgentRequestCh <- inv:
		return nil
	}
}

func (s *Service) WriteResponse(msg *proto.ClientMessage) error {
	select {
	case <-s.doneCh:
		return cb.WrapNonRetryableError(fmt.Errorf("writing response: %w", hzerrors.ErrClientNotActive))
	case s.responseCh <- msg:
		return nil
	}
}

func (s *Service) Remove(correlationID int64) error {
	select {
	case <-s.doneCh:
		return cb.WrapNonRetryableError(fmt.Errorf("removing correlation: %w", hzerrors.ErrClientNotActive))
	case s.removeCh <- correlationID:
		return nil
	}
}

func (s *Service) processIncoming() {
loop:
	for {
		select {
		case inv := <-s.requestCh:
			s.sendInvocation(inv)
		case inv := <-s.urgentRequestCh:
			s.sendInvocation(inv)
		case msg := <-s.responseCh:
			s.handleClientMessage(msg)
		case id := <-s.removeCh:
			s.removeCorrelationID(id)
		case e := <-s.groupLostCh:
			s.handleGroupLost(e)
		case <-s.doneCh:
			break loop
		}
	}
	// remove invocations
	for _, invocation := range s.invocations {
		invocation.Close()
	}
	s.invocations = nil
}

func (s *Service) sendInvocation(invocation Invocation) {
	s.logger.Trace(func() string {
		return fmt.Sprintf("invocation.Service.sendInvocation correlationID: %d", invocation.Request().CorrelationID())
	})
	s.registerInvocation(invocation)
	corrID := invocation.Request().CorrelationID()
	gid, err := s.handler.Invoke(invocation)
	if err != nil {
		s.handleError(corrID, err)
		return
	}
	invocation.SetGroup(gid)
}

func (s *Service) handleClientMessage(msg *proto.ClientMessage) {
	correlationID := msg.CorrelationID()
	if msg.Err != nil {
		s.handleError(correlationID, msg.Err)
		return
	}
	if msg.HasEventFlag() || msg.HasBackupEventFlag() {
		if inv, found := s.invocations[correlationID]; !found {
			s.logger.Trace(func() string {
				return fmt.Sprintf("invocation with unknown correlation ID: %d", correlationID)
			})
		} else if inv.EventHandler() != nil {
			handler := func() {
				inv.EventHandler()(msg)
			}
			partitionID := msg.PartitionID()
			// no specific partition (-1) are dispatched randomly in dispatch func.
			ok := s.executor.dispatch(int(partitionID), handler)
			if !ok {
				s.logger.Warnf("event could not be processed, corresponding queue is full. PartitionID: %d, CorrelationID: %d", partitionID, correlationID)
			}
		}
		return
	}
	if inv := s.unregisterInvocation(correlationID); inv != nil {
		inv.Complete(msg)
	} else {
		s.logger.Trace(func() string {
			return fmt.Sprintf("no invocation found with the correlation ID: %d", correlationID)
		})
	}
}

func (s *Service) removeCorrelationID(id int64) {
	delete(s.invocations, id)
}

func (s *Service) handleError(correlationID int64, invocationErr error) {
	if inv := s.unregisterInvocation(correlationID); inv != nil {
		s.logger.Trace(func() string {
			return fmt.Sprintf("error invoking %d: %s", correlationID, invocationErr)
		})
		if time.Now().After(inv.Deadline()) {
			invocationErr = cb.WrapNonRetryableError(invocationErr)
		}
		inv.Complete(&proto.ClientMessage{Err: invocationErr})
	} else {
		s.logger.Trace(func() string {
			return fmt.Sprintf("cannot handle error: no invocation found with correlation id: %d (%s)", correlationID, invocationErr.Error())
		})
	}
}

func (s *Service) registerInvocation(invocation Invocation) {
	message := invocation.Request()
	if message == nil {
		panic("message loaded from invocation request is nil")
	}
	message.SetPartitionId(invocation.PartitionID())
	s.invocations[message.CorrelationID()] = invocation
}

func (s *Service) unregisterInvocation(correlationID int64) Invocation {
	if invocation, ok := s.invocations[correlationID]; ok {
		if invocation.EventHandler() == nil {
			// invocations with event handlers are removed with RemoveListener functions
			s.removeCorrelationID(correlationID)
		}
		return invocation
	}
	return nil
}

func (s *Service) handleGroupLost(e *GroupLostEvent) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	if !s.running {
		return
	}
	for corrID, inv := range s.invocations {
		if inv.Group() == e.GroupID && !inv.Request().HasEventFlag() {
			s.handleError(corrID, e.Err)
		}
	}
}
