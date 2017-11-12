package internal

import (
	"fmt"
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"
	"log"
	"sync/atomic"
	"time"
)

const RETRY_WAIT_TIME_IN_SECONDS = 1

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
type ConnectionAndError struct {
	connection *Connection
	error      error
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
	client                    *HazelcastClient
	quit                      chan struct{}
	nextCorrelation           int64
	responseWaitings          map[int64]*Invocation
	eventHandlers             map[int64]*Invocation
	sending                   chan *Invocation
	responseChannel           chan *ClientMessage
	cleanupConnectionChannel  chan *ConnectionAndError
	removeEventHandlerChannel chan int64
	notSentMessages           chan int64
	invoke                    func(*Invocation)
	sendToConnectionChannel   chan *invocationConnection
}
type invocationConnection struct {
	invocation *Invocation
	connection *Connection
}

func NewInvocationService(client *HazelcastClient) *InvocationService {
	service := &InvocationService{client: client, sending: make(chan *Invocation, 10000), responseWaitings: make(map[int64]*Invocation),
		eventHandlers:   make(map[int64]*Invocation),
		responseChannel: make(chan *ClientMessage, 1),
		quit:            make(chan struct{}, 0),
		cleanupConnectionChannel:  make(chan *ConnectionAndError, 1),
		sendToConnectionChannel:   make(chan *invocationConnection, 100),
		removeEventHandlerChannel: make(chan int64, 1),
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
	return invocationService.sendInvocation(invocation)
}

func (invocationService *InvocationService) InvokeOnRandomTarget(request *ClientMessage) InvocationResult {
	invocation := NewInvocation(request, -1, nil, nil)
	return invocationService.sendInvocation(invocation)
}

func (invocationService *InvocationService) InvokeOnTarget(request *ClientMessage, target *Address) InvocationResult {
	invocation := NewInvocation(request, -1, target, nil)
	return invocationService.sendInvocation(invocation)
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
		case connectionAndErr := <-invocationService.cleanupConnectionChannel:
			invocationService.cleanupConnectionInternal(connectionAndErr.connection, connectionAndErr.error)
		case correlationId := <-invocationService.removeEventHandlerChannel:
			invocationService.removeEventHandlerInternal(correlationId)
		case invocationConnection := <-invocationService.sendToConnectionChannel:
			invocationService.sendToConnection(invocationConnection.invocation, invocationConnection.connection)
		case <-invocationService.quit:
			invocationService.quitInternal()
			return
		}
	}
}
func (invocationService *InvocationService) quitInternal() {
	for _, invocation := range invocationService.responseWaitings {
		invocation.err <- NewHazelcastClientNotActiveError("client has been shutdown", nil)
	}
}
func (invocationService *InvocationService) sendToRandomAddress(invocation *Invocation) {
	var target *Address = invocationService.client.LoadBalancer.NextAddress()
	invocationService.sendToAddress(invocation, target)
}
func (invocationService *InvocationService) invokeSmart(invocation *Invocation) {
	if invocation.boundConnection != nil {
		invocationService.sendToConnectionChannel <- &invocationConnection{invocation: invocation, connection: invocation.boundConnection}
	} else if invocation.partitionId != -1 {
		if target, ok := invocationService.client.PartitionService.PartitionOwner(invocation.partitionId); ok {
			invocationService.sendToAddress(invocation, target)
		} else {
			invocationService.handleException(invocation,
				NewHazelcastIOError(fmt.Sprintf("Partition does not have an owner. partitionId: %d", invocation.partitionId), nil))

		}
	} else if invocation.address != nil {
		invocationService.sendToAddress(invocation, invocation.address)
	} else {
		invocationService.sendToRandomAddress(invocation)
	}
}

func (invocationService *InvocationService) invokeNonSmart(invocation *Invocation) {
	if invocation.boundConnection != nil {
		invocationService.sendToConnectionChannel <- &invocationConnection{invocation: invocation, connection: invocation.boundConnection}
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
			invocationService.sendToConnectionChannel <- &invocationConnection{invocation: invocation, connection: connection}
		case err := <-errorChannel:
			log.Println("the following error occured while trying to send the invocation ", err)
			invocationService.handleException(invocation, err)
		}
	}()
}
func (invocationService *InvocationService) sendInvocation(invocation *Invocation) InvocationResult {
	invocationService.sending <- invocation
	return invocation
}
func (invocationService *InvocationService) InvokeOnConnection(request *ClientMessage, connection *Connection) InvocationResult {
	invocation := NewInvocation(request, -1, nil, connection)
	return invocationService.sendInvocation(invocation)
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
	log.Println("no invocation has been found with the correlation id: ", correlationId)
	return nil, false
}

func (invocationService *InvocationService) handleNotSentInvocation(correlationId int64) {
	if invocation, ok := invocationService.unRegisterInvocation(correlationId); ok {
		invocationService.sendInvocation(invocation)
	}
}
func (invocationService *InvocationService) removeEventHandler(correlationId int64) {
	invocationService.removeEventHandlerChannel <- correlationId
}
func (invocationService *InvocationService) removeEventHandlerInternal(correlationId int64) {
	if _, ok := invocationService.eventHandlers[correlationId]; ok {
		delete(invocationService.eventHandlers, correlationId)
	}
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
func (invocationService *InvocationService) onConnectionClosed(connection *Connection, cause error) {
	invocationService.cleanupConnection(connection, cause)
}
func (invocationService *InvocationService) onConnectionOpened(connection *Connection) {
}
func (invocationService *InvocationService) cleanupConnection(connection *Connection, cause error) {
	invocationService.cleanupConnectionChannel <- &ConnectionAndError{connection: connection, error: cause}
}

func (invocationService *InvocationService) cleanupConnectionInternal(connection *Connection, cause error) {
	for _, invocation := range invocationService.responseWaitings {
		if invocation.sentConnection == connection {
			invocationService.handleException(invocation, cause)
		}
	}
}
func (invocationService *InvocationService) handleException(invocation *Invocation, err error) {
	if !invocationService.client.LifecycleService.isLive {
		invocation.err <- NewHazelcastClientNotActiveError(err.Error(), err)
		return
	}
	if invocationService.isNotAllowedToRetryOnConnection(invocation, err) {
		invocation.err <- err
		return
	}
	//TODO:: Check invocation timeout

	if invocationService.shouldRetryInvocation(invocation, err) {
		if invocation.boundConnection != nil {
			return
		}
		go func() {
			time.Sleep(RETRY_WAIT_TIME_IN_SECONDS * time.Second)
			invocationService.sending <- invocation
		}()
		return
	}
	invocation.err <- err
}

func (invocationService *InvocationService) IsRedoOperation() bool {
	return invocationService.client.ClientConfig.ClientNetworkConfig.IsRedoOperation()
}
func (invocationService *InvocationService) shouldRetryInvocation(clientInvocation *Invocation, err error) bool {
	_, isTargetDisconnectedError := err.(*HazelcastTargetDisconnectedError)
	if (isTargetDisconnectedError && clientInvocation.request.IsRetryable) || invocationService.IsRedoOperation() || isRetrySafeError(err) {
		return true
	}
	return false
}
func isRetrySafeError(err error) bool {
	var isRetrySafe bool = false
	_, ok := err.(*HazelcastInstanceNotActiveError)
	isRetrySafe = isRetrySafe || ok
	_, ok = err.(*HazelcastTargetNotMemberError)
	isRetrySafe = isRetrySafe || ok
	_, ok = err.(*HazelcastIOError)
	isRetrySafe = isRetrySafe || ok
	//TODO:: add other errors
	return isRetrySafe
}
func (invocationService *InvocationService) isNotAllowedToRetryOnConnection(invocation *Invocation, err error) bool {
	_, isIOError := err.(*HazelcastIOError)
	if invocation.isBoundToSingleConnection() && isIOError {
		return true
	}
	_, isTargetNotMemberError := err.(*HazelcastTargetNotMemberError)
	if invocation.address != nil && isTargetNotMemberError && invocationService.client.ClusterService.GetMember(invocation.address) == nil {
		return true
	}
	return false
}
func (invocation *Invocation) isBoundToSingleConnection() bool {
	return invocation.boundConnection != nil
}
func (invocationService *InvocationService) shutdown() {
	close(invocationService.quit)
}
