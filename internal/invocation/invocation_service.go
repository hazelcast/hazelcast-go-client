package invocation

import (
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"
	ihzerror "github.com/hazelcast/hazelcast-go-client/v4/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/bufutil"
)

type ServiceCreationBundle struct {
	Handler      Handler
	RequestCh    <-chan Invocation
	ResponseCh   <-chan *proto.ClientMessage
	SmartRouting bool
	Logger       logger.Logger
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
	logger            logger.Logger
}

func NewServiceImpl(bundle ServiceCreationBundle) *Service {
	bundle.Check()
	handler := bundle.Handler
	if handler == nil {
		handler = &DefaultHandler{}
	}
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
	if msg.Err != nil {
		s.logger.Error(msg.Err)
		if msg.StartFrame != nil {
			s.handleError(msg.CorrelationID(), msg.Err)
		} else {
			panic("implement me: handleClientMessage")
		}
		return
	}
	correlationID := msg.CorrelationID()
	if msg.StartFrame.HasEventFlag() || msg.StartFrame.HasBackupEventFlag() {
		if inv, found := s.invocations[correlationID]; !found {
			s.logger.Tracef(func() (string, []interface{}) {
				return "invocation with unknown correlation id: %d", []interface{}{correlationID}
			})
		} else if inv.EventHandler() != nil {
			go inv.EventHandler()(msg)
		}
		return
	}
	if inv := s.unregisterInvocation(correlationID); inv != nil {
		if msg.Type() == int32(bufutil.MessageTypeException) {
			err := ihzerror.CreateHazelcastError(codec.DecodeError(msg))
			s.handleError(correlationID, err)
		} else {
			inv.Complete(msg)
		}
	} else {
		s.logger.Tracef(func() (string, []interface{}) {
			return "no invocation found with the correlation id: %d", []interface{}{correlationID}
		})
	}
}

func (s *Service) handleError(correlationID int64, invocationErr error) {
	if inv := s.unregisterInvocation(correlationID); inv != nil {
		s.logger.Error(invocationErr)
		inv.Complete(&proto.ClientMessage{Err: invocationErr})
	} else {
		s.logger.Tracef(func() (string, []interface{}) {
			return "cannot handle error: no invocation found with correlation id: %d", []interface{}{correlationID}
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
