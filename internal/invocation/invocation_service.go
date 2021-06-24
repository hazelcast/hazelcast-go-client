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

	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type Handler interface {
	Invoke(invocation Invocation) (groupID int64, err error)
}

type Service struct {
	requestCh       <-chan Invocation
	urgentRequestCh <-chan Invocation
	responseCh      <-chan *proto.ClientMessage
	// removeCh carries correlationIDs to be removed
	removeCh               <-chan int64
	doneCh                 chan struct{}
	invocations            map[int64]Invocation
	groupIDToCorrelationID map[int64][]int64
	correlationIDToGroupID map[int64]int64
	handler                Handler
	logger                 ilogger.Logger
}

func NewService(
	requestCh <-chan Invocation,
	urgentRequestCh <-chan Invocation,
	responseCh <-chan *proto.ClientMessage,
	removeCh <-chan int64,
	handler Handler,
	logger ilogger.Logger) *Service {
	service := &Service{
		requestCh:       requestCh,
		urgentRequestCh: urgentRequestCh,
		responseCh:      responseCh,
		removeCh:        removeCh,
		doneCh:          make(chan struct{}),
		invocations:     map[int64]Invocation{},
		//groupIDToCorrelationID: map[int64][]int64{},
		//correlationIDToGroupID: map[int64]int64{},
		handler: handler,
		logger:  logger,
	}
	go service.processIncoming()
	return service
}

func (s *Service) Stop() {
	close(s.doneCh)
	for _, inv := range s.invocations {
		inv.Close()
	}
	s.invocations = nil
}

func (s *Service) SetHandler(handler Handler) {
	s.handler = handler
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
	if _, err := s.handler.Invoke(invocation); err != nil {
		s.handleError(corrID, err)
	}
	/*
		s.correlationIDToGroupID[corrID] = groupID
		s.groupIDToCorrelationID[groupID] = append(s.groupIDToCorrelationID[groupID], corrID)

	*/
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

func (s *Service) removeCorrelationID(id int64) {
	delete(s.invocations, id)
}

func (s *Service) handleError(correlationID int64, invocationErr error) {
	if inv := s.unregisterInvocation(correlationID); inv != nil {
		s.logger.Trace(func() string {
			return fmt.Sprintf("error invoking %d: %s", correlationID, invocationErr)
		})
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
