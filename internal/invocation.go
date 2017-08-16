package internal

import (
	"sync/atomic"

	"time"

	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"

	"errors"
	"sync"
)

type Invocation struct {
	boundConnection *Connection
	sentConnection  *Connection
	address         *Address
	request         *ClientMessage
	partitionId     int32
	response        chan *ClientMessage
	closed          chan bool
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
		response:        make(chan *ClientMessage, 0),
		err:             make(chan error, 0),
		closed:          make(chan bool, 0),
		timeout:         time.After(DEFAULT_INVOCATION_TIMEOUT)}
}

func (invocation *Invocation) Result() (*ClientMessage, error) {
	select {
	case response := <-invocation.response:
		return response, nil
	case err := <-invocation.err:
		return nil, err
	case <-invocation.closed:
		return nil, errors.New("Connection is closed")
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
	lock             sync.RWMutex
}

func NewInvocationService(client *HazelcastClient) *InvocationService {
	service := &InvocationService{client: client, sending: make(chan *Invocation, 0), responseWaitings: make(map[int64]*Invocation),
		responseChannel: make(chan *ClientMessage, 0),
		quit:            make(chan bool, 0),
	}
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
	go func() {
		invocationService.sending <- invocation
	}()
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
	//partitionId := int32(-1)
	return invocationService.InvokeOnPartitionOwner(request, partitionId)
}

func (invocationService *InvocationService) process() {
	for {
		select {
		case invocation := <-invocationService.sending:
			//TODO
			go invocationService.invoke(invocation)
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
			invocation.err <- errors.New("Partition table doesnt contain the address for the given partition id")
			//TODO should I handle this case
		}
	} else if invocation.address != nil {
		//TODO :: Partition id is -1 but it cant be -1
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
		case connection, alive := <-connectionChannel:
			if !alive {
				//TODO :: Handle the case if the connection is closed
			} else {
				err := connection.Send(invocation.request)
				if err != nil {
					//not sent
					invocationService.notSentMessages <- invocation.request.CorrelationId()
				} else {
					invocation.sentConnection = connection
					invocation.closed = connection.closed
				}

			}
		}
	}()
}
func (invocationService *InvocationService) InvokeOnConnection(request *ClientMessage, connection *Connection) InvocationResult {
	invocation := NewInvocation(request, -1, nil, connection)
	invocationService.sending <- invocation
	return invocation
}
func (invocationService *InvocationService) sendToConnection(invocation *Invocation, connection *Connection) {
	invocationService.registerInvocation(invocation)

	err := connection.Send(invocation.request)
	if err != nil {
		//not sent
		invocationService.notSentMessages <- invocation.request.CorrelationId()
	}
	invocation.sentConnection = connection
	invocation.closed = connection.closed
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
	message.SetFlags(BEGIN_END_FLAG)
	invocationService.lock.Lock()
	invocationService.responseWaitings[correlationId] = invocation
	invocationService.lock.Unlock()
}

func (invocationService *InvocationService) unRegisterInvocation(correlationId int64) (*Invocation, bool) {
	invocationService.lock.Lock()
	defer invocationService.lock.Unlock()
	if invocation, ok := invocationService.responseWaitings[correlationId]; ok {
		defer delete(invocationService.responseWaitings, correlationId)
		return invocation, ok
	}
	//TODO::HANDLE no invocation found with correleationID
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
