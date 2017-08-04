package internal

import (
	"sync/atomic"

	"time"

	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"
)

type Invocation struct {
	boundConnection *Connection
	sentConnection  *Connection
	address         *Address
	request         *ClientMessage
	partitionId     int32
	response        chan *ClientMessage
	err             chan error
	timeout         <-chan time.Time //TODO invocation should be sent in this timeout
}

type InvocationResult interface {
	Result() (*ClientMessage, error)
}

func NewInvocation(request *ClientMessage, partitionId int32, address *Address, connection *Connection) *Invocation {
	return &Invocation{
		request:         request,
		partitionId:     partitionId,
		address:         address,
		boundConnection: connection,
		response:        make(chan *ClientMessage),
		err:             make(chan error),
		timeout:         time.After(DEFAULT_INVOCATION_TIMEOUT)}
}

func (invocation *Invocation) Result() (*ClientMessage, error) {
	select {
	case response := <-invocation.response:
		return response, nil
	case err := <-invocation.err:
		return nil, err
	}
}

type InvocationService struct {
	client           *HazelcastClient
	quit             chan bool
	nextCorrelation  int64
	responseWaitings map[int64]*Invocation
	sending          chan *Invocation
	responseChannel  chan *ClientMessage
	notSentMessages  chan int64
	invoke           func(*Invocation)
}

func NewInvocationService(client *HazelcastClient) *InvocationService {
	service := &InvocationService{client: client, sending: make(chan *Invocation, 1000)}
	//if client.config.IsSmartRouting() {
	service.invoke = service.invokeSmart
	//} else {
	//	service.invoke = service.invokeNonSmart
	//}
	service.start()
	return service
}
func (invocationService *InvocationService) start() {
	go invocationService.process()
}

func (invocationService *InvocationService) nextCorrelationId() int64 {
	invocationService.nextCorrelation = atomic.AddInt64(&invocationService.nextCorrelation, 1)
	return invocationService.nextCorrelation
}

func (invocationService *InvocationService) InvokeOnPartitionOwner(request *ClientMessage, partitionId int32) InvocationResult {
	invocation := NewInvocation(request, partitionId, nil, nil)
	invocationService.sending <- invocation
	return invocation
}

func (invocationService *InvocationService) InvokeOnRandomTarget(request *ClientMessage) InvocationResult {
	invocation := NewInvocation(request, -1, nil, nil)
	invocationService.sending <- invocation
	return invocation
}

func (invocationService *InvocationService) InvokeOnTarget(request *ClientMessage, target *Address) InvocationResult {
	invocation := NewInvocation(request, -1, target, nil)
	invocationService.sending <- invocation
	return invocation
}

func (invocationService *InvocationService) InvokeOnKeyOwner(request *ClientMessage, keyData *serialization.Data) InvocationResult {
	partitionId := invocationService.client.PartitionService.GetPartitionId(keyData)
	return invocationService.InvokeOnPartitionOwner(request, partitionId)
}

func (invocationService *InvocationService) process() {
	for {
		select {
		case invocation := <-invocationService.sending:
			//TODO
			invocationService.invoke(invocation)
		case response := <-invocationService.responseChannel:
			invocationService.handleResponse(response)
		case correlationId := <-invocationService.notSentMessages:
			invocationService.handleNotSentInvocation(correlationId)
		case <-invocationService.quit:
			return
		}
	}
}

func (invocationService *InvocationService) invokeSmart(invocation *Invocation) {
	if invocation.boundConnection != nil {
		invocationService.sendToConnection(invocation, invocation.boundConnection)
	} else if invocation.partitionId != -1 {
		if target, ok := invocationService.client.PartitionService.PartitionOwner(invocation.partitionId); ok {
			invocationService.sendToAddress(invocation, target)
		} else {
			//TODO should I handle this case
		}
	} else if invocation.address != nil {
		invocationService.sendToAddress(invocation, invocation.address)
	} else {
		var target *Address = invocationService.client.LoadBalancer.NextAddress()
		invocationService.sendToAddress(invocation, target)
	}
}

func (invocationService *InvocationService) invokeNonSmart(invocation *Invocation) {
	//TODO implement
}

func (invocationService *InvocationService) send(invocation *Invocation, connectionChannel chan *Connection) {
	invocationService.registerInvocation(invocation)

	go func() {
		select {
		case <-invocationService.quit:
			return
		case connection := <-connectionChannel:
			err := connection.Send(invocation.request)
			if err != nil {
				//not sent
				invocationService.notSentMessages <- invocation.request.CorrelationId()
			}
			invocation.sentConnection = connection
		}
	}()
}

func (invocationService *InvocationService) sendToConnection(invocation *Invocation, connection *Connection) {
	invocationService.registerInvocation(invocation)

	err := connection.Send(invocation.request)
	if err != nil {
		//not sent
		invocationService.notSentMessages <- invocation.request.CorrelationId()
	}
	invocation.sentConnection = connection
}

func (invocationService *InvocationService) sendToAddress(invocation *Invocation, address *Address) {
	connectionChannel := invocationService.client.ConnectionManager.GetConnection(address)
	invocationService.send(invocation, connectionChannel)
}

func (invocationService *InvocationService) registerInvocation(invocation *Invocation) {
	message := invocation.request
	correlationId := invocationService.nextCorrelationId()
	message.SetCorrelationId(correlationId)
	message.SetPartitionId(invocation.partitionId)
	invocationService.responseWaitings[correlationId] = invocation
}

func (invocationService *InvocationService) unRegisterInvocation(correlationId int64) (*Invocation, bool) {
	if invocation, ok := invocationService.responseWaitings[correlationId]; ok {
		defer delete(invocationService.responseWaitings, correlationId)
		return invocation, ok
	}
	//HANDLE no invocation found with correleationID
	return nil, false
}

func (invocationService *InvocationService) handleNotSentInvocation(correlationId int64) {
	if invocation, ok := invocationService.unRegisterInvocation(correlationId); ok {
		invocationService.sending <- invocation
	}
}

func (invocationService *InvocationService) handleResponse(response *ClientMessage) {
	if invocation, ok := invocationService.unRegisterInvocation(response.CorrelationId()); ok {
		if response.MessageType() == MESSAGE_TYPE_EXCEPTION {
			invocation.err <- convertToError(response)
		} else {
			invocation.response <- response
		}
	}
}

func convertToError(clientMessage *ClientMessage) *Error {
	return ErrorCodecDecode(clientMessage)
}

//func (invocationService *InvocationService) retry(correlationId int64) {
//
//}

//func (invocationService *InvocationService) shouldRetryInvocation(clientInvocation *Invocation, err error) bool {
//
//}
