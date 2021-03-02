package invocation

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/partition"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/bufutil"
	"sync"
	"sync/atomic"
	"time"
)

type Service interface {
	Send(invocation Invocation) Result
	//InvokeOnPartitionOwner(message *proto.ClientMessage, partitionID int32) Result
	//InvokeOnRandomTarget(message *proto.ClientMessage) Result
	//InvokeOnKeyOwner(message *proto.ClientMessage, data serialization.Data) Result
	//InvokeOnTarget(message *proto.ClientMessage, address *core.Address) Result
	//invokeOnConnection(message *proto.ClientMessage, connection *Connection) invocationResult
	//CleanupConnection(connection *connection.Impl, connErr error)
	//removeEventHandler(correlationID int64)
	//sendInvocation(invocation *invocation) invocationResult
	//InvocationTimeout() time.Duration
	//handleResponse(response interface{})
	//shutdown()
}

type ServiceImpl struct {
	nextCorrelation   int64
	invocationsLock   *sync.RWMutex
	invocations       map[int64]Invocation
	invocationTimeout time.Duration
	retryPause        time.Duration
	eventHandlersLock *sync.RWMutex
	eventHandlers     map[int64]EventHandler
	responseChannel   chan *proto.ClientMessage
	shutDown          atomic.Value
	smartRouting      bool
	handler           Handler
	logger            logger.Logger

	partitionService partition.Service
}

func NewServiceImpl(bundle CreationBundle) *ServiceImpl {
	bundle.Check()
	service := &ServiceImpl{
		partitionService:  bundle.PartitionService,
		invocationsLock:   &sync.RWMutex{},
		invocations:       map[int64]Invocation{},
		invocationTimeout: 120 * time.Second,
		retryPause:        1 * time.Second,
		eventHandlersLock: &sync.RWMutex{},
		eventHandlers:     map[int64]EventHandler{},
		responseChannel:   make(chan *proto.ClientMessage, 1),
		smartRouting:      bundle.SmartRouting,
		handler:           bundle.Handler,
		logger:            bundle.Logger,
	}
	service.shutDown.Store(false)
	go service.startProcess()
	return service
}

func (s *ServiceImpl) Send(invocation Invocation) Result {
	return s.sendInvocation(invocation)
}

/*
func (s *ServiceImpl) ConnectionOpened(conn *connection.Impl) {
	panic("implement me!")
}

func (s *ServiceImpl) ConnectionClosed(conn *connection.Impl, err error) {
	panic("implement me!")
}

func (s *ServiceImpl) CleanupConnection(conn *connection.Impl, connErr error) {
	panic("implement me!")
}
*/

func (s *ServiceImpl) startProcess() {
	for msg := range s.responseChannel {
		if msg.Err != nil {
			panic("implement me!")
		}

	}
}

func (s *ServiceImpl) sendInvocation(invocation Invocation) Result {
	if s.shutDown.Load() == true {
		invocation.CompleteWithErr(core.NewHazelcastClientNotActiveError("client is shut down", nil))
	}
	s.registerInvocation(invocation)
	if err := s.handler.Invoke(invocation); err != nil {
		s.handleError(invocation, err)
	}
	return invocation
}

func (s *ServiceImpl) handleClientMessage(msg *proto.ClientMessage) {
	correlationID := msg.GetCorrelationID()
	if msg.StartFrame.HasEventFlag() || msg.StartFrame.HasBackupEventFlag() {
		s.eventHandlersLock.RLock()
		handler, found := s.eventHandlers[correlationID]
		s.eventHandlersLock.RUnlock()
		if !found {
			s.logger.Trace("event message with unknown correlation id: ", correlationID)
		} else {
			handler(msg)
		}
		return
	}
	if invocation := s.unregisterInvocation(correlationID); invocation != nil {
		if msg.GetMessageType() == int32(bufutil.MessageTypeException) {
			err := internal.CreateHazelcastError(msg.DecodeError())
			s.handleError(invocation, err)
		} else {
			invocation.Complete(msg)
		}
	} else {
		s.logger.Trace("no invocation found with the correlation id: ", correlationID)
	}
}

func (s *ServiceImpl) handleError(invocation Invocation, err error) {
	correlationID := invocation.Request().GetCorrelationID()
	if inv := s.unregisterInvocation(correlationID); inv != nil {
		panic("implement me!")
	} else {
		s.logger.Trace("no invocation found with the correlation id: ", correlationID)
	}
}

func (s *ServiceImpl) registerInvocation(invocation Invocation) {
	message := invocation.Request()
	if message == nil {
		panic("message loaded from invocation request is nil")
	}
	correlationID := s.nextCorrelationID()
	message.SetCorrelationID(correlationID)
	message.SetPartitionId(invocation.PartitionID())
	if invocation.EventHandler() != nil {
		s.eventHandlersLock.Lock()
		s.eventHandlers[correlationID] = invocation.EventHandler()
		s.eventHandlersLock.Unlock()
	}
	s.invocationsLock.Lock()
	s.invocations[correlationID] = invocation
	s.invocationsLock.Unlock()
}

func (s *ServiceImpl) unregisterInvocation(correlationID int64) Invocation {
	s.invocationsLock.Lock()
	defer s.invocationsLock.Unlock()
	if invocation, ok := s.invocations[correlationID]; ok {
		delete(s.invocations, correlationID)
		return invocation
	}
	return nil
}

func (s *ServiceImpl) nextCorrelationID() int64 {
	return atomic.AddInt64(&s.nextCorrelation, 1)
}
