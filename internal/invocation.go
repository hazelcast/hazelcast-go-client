package internal

import (
	"sync/atomic"

	"time"

	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"

	"errors"
	"log"
	"sync"
)

type Invocation struct {
	boundConnection         *Connection
	sentConnection          *Connection
	address                 *Address
	request                 *ClientMessage
	partitionId             int32
	response                chan *ClientMessage
	closed                  chan bool
	err                     chan error
	eventHandler            func(clientMessage *ClientMessage)
	registrationId          *string
	timeout                 <-chan time.Time //TODO invocation should be sent in this timeout
	listenerResponseDecoder DecodeListenerResponse
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
		response:        make(chan *ClientMessage, 10),
		err:             make(chan error, 1),
		closed:          make(chan bool, 1),
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
	client                   *HazelcastClient
	quit                     chan struct{}
	nextCorrelation          int64
	responseWaitings         map[int64]*Invocation
	eventHandlers            map[int64]*Invocation
	sending                  chan *Invocation
	responseChannel          chan *ClientMessage
	cleanupConnectionChannel chan *Connection
	notSentMessages          chan int64
	invoke                   func(*Invocation)
	lock                     sync.RWMutex
}

func NewInvocationService(client *HazelcastClient) *InvocationService {
	service := &InvocationService{client: client, sending: make(chan *Invocation, 10000), responseWaitings: make(map[int64]*Invocation),
		eventHandlers:   make(map[int64]*Invocation),
		responseChannel: make(chan *ClientMessage, 1),
		quit:            make(chan struct{}, 0),
		cleanupConnectionChannel: make(chan *Connection, 1),
	}
	if client.ClientConfig.IsSmartRouting() {
		service.invoke = service.invokeSmart
	} else {
		service.invoke = service.invokeNonSmart
	}
	service.start()
	service.client.ConnectionManager.AddListener(service)
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
	return invocationService.SendInvocation(invocation)
}

func (invocationService *InvocationService) InvokeOnRandomTarget(request *ClientMessage) InvocationResult {
	invocation := NewInvocation(request, -1, nil, nil)
	return invocationService.SendInvocation(invocation)
}

func (invocationService *InvocationService) InvokeOnTarget(request *ClientMessage, target *Address) InvocationResult {
	invocation := NewInvocation(request, -1, target, nil)
	return invocationService.SendInvocation(invocation)
}

func (invocationService *InvocationService) InvokeOnKeyOwner(request *ClientMessage, keyData *serialization.Data) InvocationResult {
	partitionId := invocationService.client.PartitionService.GetPartitionId(keyData)
	return invocationService.InvokeOnPartitionOwner(request, partitionId)
}

func (invocationService *InvocationService) process() {
	for {
		select {
		case invocation := <-invocationService.sending:
			invocationService.invoke(invocation)
		case response := <-invocationService.responseChannel:
			invocationService.handleResponse(response)
		case correlationId := <-invocationService.notSentMessages:
			invocationService.handleNotSentInvocation(correlationId)
		case connection := <-invocationService.cleanupConnectionChannel:
			invocationService.cleanupConnectionInternal(connection)
		case <-invocationService.quit:
			return
		}
	}
}
func (invocationService *InvocationService) sendToRandomAddress(invocation *Invocation) {
	var target *Address = invocationService.client.LoadBalancer.NextAddress()
	invocationService.sendToAddress(invocation, target)
}
func (invocationService *InvocationService) invokeSmart(invocation *Invocation) {
	if invocation.boundConnection != nil {
		invocationService.sendToConnection(invocation, invocation.boundConnection)
	} else if invocation.partitionId != -1 {
		if target, ok := invocationService.client.PartitionService.PartitionOwner(invocation.partitionId); ok {
			invocationService.sendToAddress(invocation, target)
		} else {
			invocationService.sendToRandomAddress(invocation)
		}
	} else if invocation.address != nil {
		invocationService.sendToAddress(invocation, invocation.address)
	} else {
		invocationService.sendToRandomAddress(invocation)
	}
}

func (invocationService *InvocationService) invokeNonSmart(invocation *Invocation) {
	if invocation.boundConnection != nil {
		invocationService.sendToConnection(invocation, invocation.boundConnection)
	} else {
		addr := invocationService.client.ClusterService.ownerConnectionAddress
		invocationService.sendToAddress(invocation, addr)
	}
}

func (invocationService *InvocationService) send(invocation *Invocation, connectionChannel chan *Connection, errorChannel chan error) {
	go func() {
		select {
		case <-invocationService.quit:
			return
		case connection := <-connectionChannel:
			invocationService.sendToConnection(invocation, connection)
		case err := <-errorChannel:
			log.Println("the following error occured while trying to send the invocation ", err)
			//TODO::Handle error
		}
	}()
}
func (invocationService *InvocationService) SendInvocation(invocation *Invocation) InvocationResult {
	invocationService.sending <- invocation
	return invocation
}
func (invocationService *InvocationService) InvokeOnConnection(request *ClientMessage, connection *Connection) InvocationResult {
	invocation := NewInvocation(request, -1, nil, connection)
	return invocationService.SendInvocation(invocation)
}
func (invocationService *InvocationService) sendToConnection(invocation *Invocation, connection *Connection) {
	invocationService.registerInvocation(invocation)

	err := connection.Send(invocation.request)
	if err != nil {
		//not sent
		invocationService.notSentMessages <- invocation.request.CorrelationId()
	} else {
		invocation.sentConnection = connection
		invocation.closed = connection.closed
	}

}

func (invocationService *InvocationService) sendToAddress(invocation *Invocation, address *Address) {
	connectionChannel, errorChannel := invocationService.client.ConnectionManager.GetOrConnect(address)
	invocationService.send(invocation, connectionChannel, errorChannel)
}

func (invocationService *InvocationService) registerInvocation(invocation *Invocation) {
	message := invocation.request
	correlationId := invocationService.nextCorrelationId()
	message.SetCorrelationId(correlationId)
	message.SetPartitionId(invocation.partitionId)
	message.SetFlags(BEGIN_END_FLAG)
	if invocation.eventHandler != nil {
		invocationService.eventHandlers[correlationId] = invocation
	}
	invocationService.responseWaitings[correlationId] = invocation
}

func (invocationService *InvocationService) unRegisterInvocation(correlationId int64) (*Invocation, bool) {
	if invocation, ok := invocationService.responseWaitings[correlationId]; ok {
		defer delete(invocationService.responseWaitings, correlationId)
		return invocation, ok
	}
	if invocation, ok := invocationService.eventHandlers[correlationId]; ok {
		return invocation, ok
	}
	//TODO::HANDLE no invocation found with correleationID
	return nil, false
}

func (invocationService *InvocationService) handleNotSentInvocation(correlationId int64) {
	if invocation, ok := invocationService.unRegisterInvocation(correlationId); ok {
		invocationService.SendInvocation(invocation)
	}
}
func (invocationService *InvocationService) removeEventHandler(correlationId int64) error {
	if _, ok := invocationService.eventHandlers[correlationId]; ok {
		delete(invocationService.eventHandlers, correlationId)
		return nil
	}
	return errors.New("No event handler for the given correlationId")
}
func (invocationService *InvocationService) handleResponse(response *ClientMessage) {
	correlationId := response.CorrelationId()
	if invocation, ok := invocationService.unRegisterInvocation(correlationId); ok {
		if response.HasFlags(LISTENER_FLAG) > 0 {
			invocation, found := invocationService.eventHandlers[correlationId]
			if !found {
				log.Println("Got an event message with unknown correlation id")
			} else {
				invocation.eventHandler(response)
			}
			return
		}
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
func (invocationService *InvocationService) onConnectionClosed(connection *Connection) {
	invocationService.cleanupConnection(connection)
}
func (invocationService *InvocationService) onConnectionOpened(connection *Connection) {
}
func (invocationService *InvocationService) cleanupConnection(connection *Connection) {
	invocationService.cleanupConnectionChannel <- connection
}

//TODO::Add error(cause) parameter to this function
func (invocationService *InvocationService) cleanupConnectionInternal(connection *Connection) {
	for _, invocation := range invocationService.responseWaitings {
		if invocation.sentConnection == connection {
			//TODO:: send a proper error message
			invocationService.handleException(invocation, errors.New("connection Closed"))
		}
	}

	if invocationService.client.LifecycleService.isLive {
		for _, invocation := range invocationService.eventHandlers {
			if invocation.sentConnection == connection && invocation.boundConnection == nil {
				// Since reregistration is done independently,it uses different resources than invocation service
				// we dont need to wait for it.
				go invocationService.client.ListenerService.reregisterListener(invocation)
			}
		}
	}

}
func (invocationService *InvocationService) handleException(invocation *Invocation, err error) {
	if !invocationService.client.LifecycleService.isLive {
		invocation.err <- errors.New("lifecycle is not alive")
		return
	}
	if invocationService.shouldRetryInvocation(invocation, err) {
		if invocationService.tryRetry(invocation) {
			return
		}
	}
	invocation.err <- errors.New("Invocation is not retryable")
}
func (invocationService *InvocationService) tryRetry(invocation *Invocation) bool {
	if invocation.boundConnection != nil {
		return false
	}
	//TODO:: Check invocation timeout
	invocationService.sending <- invocation

	return true
}

func (invocationService *InvocationService) shouldRetryInvocation(clientInvocation *Invocation, err error) bool {
	//TODO:: implement
	return true
}
func (invocationService *InvocationService) shutdown() {
	close(invocationService.quit)
}
