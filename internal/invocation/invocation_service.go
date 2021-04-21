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

package invocation

import (
	"fmt"
	"sync/atomic"
	"time"

	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type Handler interface {
	Invoke(invocation Invocation) error
}

type ServiceCreationBundle struct {
	Handler      Handler
	RequestCh    <-chan Invocation
	ResponseCh   <-chan *proto.ClientMessage
	SmartRouting bool
	Logger       ilogger.Logger
}

func (b ServiceCreationBundle) Check() {
	// Handler can be nil
	if b.Logger == nil {
		panic("Logger is nil")
	}
	if b.RequestCh == nil {
		panic("RequestCh is nil")
	}
	if b.ResponseCh == nil {
		panic("ResponseCh is nil")
	}
}

type Service struct {
	//nextCorrelationID int64
	requestCh         <-chan Invocation
	responseCh        <-chan *proto.ClientMessage
	invocations       map[int64]Invocation
	invocationTimeout time.Duration
	retryPause        time.Duration
	shutDown          atomic.Value
	smartRouting      bool
	handler           Handler
	logger            ilogger.Logger
}

func NewServiceImpl(bundle ServiceCreationBundle) *Service {
	bundle.Check()
	handler := bundle.Handler
	service := &Service{
		requestCh:         bundle.RequestCh,
		responseCh:        bundle.ResponseCh,
		invocations:       map[int64]Invocation{},
		invocationTimeout: 120 * time.Second,
		retryPause:        1 * time.Second,
		smartRouting:      bundle.SmartRouting,
		handler:           handler,
		logger:            bundle.Logger,
	}
	service.shutDown.Store(false)
	go service.processIncoming()
	return service
}

func (s *Service) SetHandler(handler Handler) {
	s.handler = handler
}

func (s *Service) processIncoming() {
	for {
		select {
		case inv := <-s.requestCh:
			s.sendInvocation(inv)
		case msg := <-s.responseCh:
			s.handleClientMessage(msg)
		}
	}
}

func (s *Service) sendInvocation(invocation Invocation) Result {
	s.registerInvocation(invocation)
	if err := s.handler.Invoke(invocation); err != nil {
		s.handleError(invocation.Request().CorrelationID(), err)
	}
	return invocation
}

func (s *Service) handleClientMessage(msg *proto.ClientMessage) {
	correlationID := msg.CorrelationID()
	if msg.Err != nil {
		s.handleError(correlationID, msg.Err)
		return
	}
	if msg.StartFrame.HasEventFlag() || msg.StartFrame.HasBackupEventFlag() {
		if inv, found := s.invocations[correlationID]; !found {
			s.logger.Trace(func() string {
				return fmt.Sprintf("invocation with unknown correlation ID: %d", correlationID)
			})
		} else if inv.EventHandler() != nil {
			go inv.EventHandler()(msg)
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

func (s *Service) handleError(correlationID int64, invocationErr error) {
	if inv := s.unregisterInvocation(correlationID); inv != nil {
		s.logger.Error(invocationErr)
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
			// XXX: we don't remove invocations with event handlers.
			// that may leak memory
			delete(s.invocations, correlationID)
		}
		return invocation
	}
	return nil
}
