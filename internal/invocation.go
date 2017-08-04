package internal

import (
	"sync/atomic"

	"github.com/hazelcast/go-client"
)

type Invocation struct {
	boundConnection *Connection
	address         *hazelcast.Address
	request         *ClientMessage
	partitionId     int32
	response        chan *ClientMessage
	err             chan error
}

type InvocationResult interface {
	Result() (*ClientMessage, error)
}

func NewInvocation(request *ClientMessage) *Invocation {
	return NewInvocationWithPartitionId(request, -1)
}

func NewInvocationWithPartitionId(request *ClientMessage, partitionId int32) *Invocation {
	return &Invocation{request: request, partitionId: partitionId, response: make(chan *ClientMessage)}
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
	client          *HazelcastClient
	quit            chan bool
	pending         map[int64]*Invocation
	nextCorrelation int64
	sending         chan *Invocation
	response        chan *ClientMessage
	sendingError    chan int64
	invoke          func(*Invocation)
}

func NewInvocationService(client *HazelcastClient) *InvocationService {
	service := &InvocationService{client: client, sending: make(chan *Invocation, 1000)}
	if client.config.IsSmartRouting() {
		service.invoke = service.invokeSmart
	} else {
		service.invoke = service.invokeNonSmart
	}
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
	invocation := &Invocation{request: request, partitionId: partitionId}
	invocationService.sending <- invocation
	return invocation
}

func (invocationService *InvocationService) InvokeOnRandomTarget(request *ClientMessage) InvocationResult {
	invocation := &Invocation{request: request, partitionId: -1}
	invocationService.sending <- invocation
	return invocation
}

func (invocationService *InvocationService) InvokeOnTarget(request *ClientMessage, target *hazelcast.Address) InvocationResult {
	invocation := &Invocation{request: request, address: target}
	invocationService.sending <- invocation
	return invocation
}

func (invocationService *InvocationService) InvokeOnKeyOwner(request *ClientMessage, keyData *hazelcast.Data) InvocationResult {
	partitionId := invocationService.client.partitionService.GetPartitionId(keyData)
	return invocationService.InvokeOnPartitionOwner(request, partitionId)
}

func (invocationService *InvocationService) process() {
	for {
		select {
		case invocation := <-invocationService.sending:
			//TODO
			invocationService.invoke(invocation)
		case response := <-invocationService.response:
			invocationService.handleResponse(response)
		case correlationId := <-invocationService.sendingError:
			invocationService.handleNotSentInvocation(correlationId)
		case <-invocationService.quit:
			return
		}
	}
}

func (invocationService *InvocationService) invokeSmart(invocation *Invocation) {
	if invocation.boundConnection != nil {
		invocationService.send(invocation, invocation.boundConnection)
	} else if invocation.partitionId != -1 {
		//TODO PARTITION SERVICE
		if target, ok := invocationService.client.partitionService.PartitionOwner(invocation.partitionId); ok {
			invocationService.sendToAddress(invocation, target)
		} else {
			//TODO should I handle this case
		}
	} else if invocation.address != nil {
		invocationService.sendToAddress(invocation, invocation.address)
	} else {
		var target *hazelcast.Address = invocationService.client.loadBalancer.NextAddress()
		invocationService.sendToAddress(invocation, target)
	}
}

func (invocationService *InvocationService) invokeNonSmart(invocation *Invocation) {
	//TODO implement
}

func (invocationService *InvocationService) send(invocation *Invocation, connection *Connection) {
	message := invocation.request
	correlationId := invocationService.nextCorrelationId()
	message.SetCorrelationId(correlationId)
	message.SetPartitionId(invocation.partitionId)
	invocationService.pending[correlationId] = invocation

}

func (invocationService *InvocationService) sendToAddress(invocation *Invocation, address *hazelcast.Address) {

}

func (invocationService *InvocationService) handleNotSentInvocation(correlationId int64) {

}

func (invocationService *InvocationService) handleResponse(response *ClientMessage) {
	invocation := invocationService.pending[response.CorrelationId()]
	delete(invocationService.pending, response.CorrelationId())
	//TODO if response is ERROR, ha
	if response.MessageType() == MESSAGE_TYPE_EXCEPTION {
		invocation.err <- convertToError(response)
	} else {
		invocation.response <- response
	}
}
func convertToError(clientMessage *ClientMessage) *hazelcast.Error {
	return ErrorCodecDecode(clientMessage)
}

func (invocationService *InvocationService) retry(correlationId int64) {

}

func (invocationService *InvocationService) shouldRetryInvocation(clientInvocation *Invocation, err error) bool {

}
