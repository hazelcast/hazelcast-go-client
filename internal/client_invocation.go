package internal

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"sync/atomic"
	"time"
)

const UnassignedPartition = -1

type ClientInvocation interface {
	GetClientMessage() *proto.ClientMessage
	AddEventEventHandler(handle func(clientMessage *proto.ClientMessage))
	GetEventHandler() func(clientMessage *proto.ClientMessage)
	SetSendConnection(connection TcpClientConnection)
	Inkove() (*proto.ClientMessage, error)
}

type clientInvocation struct {
	lifecycleService      *lifecycleService
	invocationService     ClientInvocationService
	request               atomic.Value
	response              chan interface{}
	callIdSequence        int64
	isComplete            int32
	connection            TcpClientConnection
	sentConnection        atomic.Value
	uuid                  core.UUID
	partitionID           int32
	eventHandler          func(clientMessage *proto.ClientMessage)
	deadline              time.Time
	isSmartRoutingEnabled bool
}

func NewClientInvocation(client *HazelcastClient, request *proto.ClientMessage, connection TcpClientConnection) ClientInvocation {
	invocation := &clientInvocation{
		lifecycleService:      client.lifecycleService,
		invocationService:     client.getClientInvocationService(),
		partitionID:           UnassignedPartition,
		connection:            connection,
		response:              make(chan interface{}, 1),
		isComplete:            0,
		deadline:              time.Now().Add(client.InvocationService.InvocationTimeout()),
		isSmartRoutingEnabled: client.getClientInvocationService().IsSmartRoutingEnabled(),
	}
	invocation.request.Store(request)
	return invocation
}

func (i *clientInvocation) GetClientMessage() *proto.ClientMessage {
	return i.request.Load().(*proto.ClientMessage)
}

func (i *clientInvocation) AddEventEventHandler(handler func(clientMessage *proto.ClientMessage)) {
	i.eventHandler = handler
}

func (i *clientInvocation) GetEventHandler() func(clientMessage *proto.ClientMessage) {
	return i.eventHandler
}

func (i *clientInvocation) SetSendConnection(connection TcpClientConnection) {
	i.sentConnection.Store(connection)
}

func (i *clientInvocation) unwrapResponse(response interface{}) (*proto.ClientMessage, error) {
	switch res := response.(type) {
	case *proto.ClientMessage:
		return res, nil
	case error:
		return nil, res
	default:
		panic("Unexpected response in invocation ")
	}
}

func (i *clientInvocation) setCorrelationId() {
	message := i.GetClientMessage()
	message.SetCorrelationID(atomic.AddInt64(&i.callIdSequence, 2))
	i.request.Store(message)
}

func (i *clientInvocation) Inkove() (*proto.ClientMessage, error) {
	i.setCorrelationId()
	i.invokeOnSelection()
	response := <-i.response
	return i.unwrapResponse(response)
}

func (i *clientInvocation) invokeOnSelection() {

	if i.connection != nil {
		i.invocationService.InvokeOnConnection(i, i.connection)
	}

	if i.isSmartRoutingEnabled {
		i.invocationService.InvokeOnPartitionOwner(i, i.partitionID)
	} else if i.uuid != nil {
		i.invocationService.InvokeOnTarget(i, i.uuid)
	} else {
		i.invocationService.Invoke(i)
	}
}
