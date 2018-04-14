// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"log"
	"sync/atomic"
	"time"
)

const RetryWaitTimeInSeconds = 1

type invocation struct {
	boundConnection         *Connection
	sentConnection          *Connection
	address                 *protocol.Address
	request                 *protocol.ClientMessage
	partitionId             int32
	response                chan *protocol.ClientMessage
	closed                  chan bool
	err                     chan error
	done                    chan bool
	eventHandler            func(clientMessage *protocol.ClientMessage)
	registrationId          *string
	timeout                 <-chan time.Time
	isTimedout              atomic.Value
	timedoutTime            atomic.Value
	listenerResponseDecoder protocol.DecodeListenerResponse
}
type connectionAndError struct {
	connection *Connection
	error      error
}
type invocationResult interface {
	Result() (*protocol.ClientMessage, error)
}

func newInvocation(request *protocol.ClientMessage, partitionId int32, address *protocol.Address, connection *Connection, client *HazelcastClient) *invocation {
	invocation := &invocation{
		request:         request,
		partitionId:     partitionId,
		address:         address,
		boundConnection: connection,
		response:        make(chan *protocol.ClientMessage, 10),
		err:             make(chan error, 1),
		closed:          make(chan bool, 1),
		done:            make(chan bool, 1),
		timeout:         time.After(client.ClientConfig.ClientNetworkConfig().InvocationTimeout()),
	}
	invocation.isTimedout.Store(false)
	go func() {
		select {
		case <-invocation.done:
			return
		case <-invocation.timeout:
			invocation.timedoutTime.Store(time.Now())
			invocation.isTimedout.Store(true)
		}
	}()
	return invocation
}

func (invocation *invocation) Result() (*protocol.ClientMessage, error) {
	select {
	case response := <-invocation.response:
		invocation.done <- true
		return response, nil
	case err := <-invocation.err:
		invocation.done <- true
		return nil, err
	}
}

type invocationService struct {
	client                      *HazelcastClient
	quit                        chan struct{}
	nextCorrelation             int64
	responseWaitings            map[int64]*invocation
	eventHandlers               map[int64]*invocation
	sending                     chan *invocation
	responseChannel             chan *protocol.ClientMessage
	cleanupConnectionChannel    chan *connectionAndError
	removeEventHandlerChannel   chan int64
	notSentMessages             chan int64
	invoke                      func(*invocation)
	sendToConnectionChannel     chan *invocationConnection
	unRegisterInvocationChannel chan int64
	isShutdown                  atomic.Value
}
type invocationConnection struct {
	invocation *invocation
	connection *Connection
}

func newInvocationService(client *HazelcastClient) *invocationService {
	service := &invocationService{client: client, sending: make(chan *invocation, 10000), responseWaitings: make(map[int64]*invocation),
		eventHandlers:   make(map[int64]*invocation),
		responseChannel: make(chan *protocol.ClientMessage, 1),
		quit:            make(chan struct{}, 0),
		cleanupConnectionChannel:    make(chan *connectionAndError, 1),
		sendToConnectionChannel:     make(chan *invocationConnection, 100),
		removeEventHandlerChannel:   make(chan int64, 1),
		notSentMessages:             make(chan int64, 10000),
		unRegisterInvocationChannel: make(chan int64, 10000),
	}

	service.isShutdown.Store(false)
	if client.ClientConfig.ClientNetworkConfig().IsSmartRouting() {
		service.invoke = service.invokeSmart
	} else {
		service.invoke = service.invokeNonSmart
	}
	service.start()
	service.client.ConnectionManager.addListener(service)
	return service
}
func (invocationService *invocationService) start() {
	go invocationService.process()
}

func (invocationService *invocationService) nextCorrelationId() int64 {
	invocationService.nextCorrelation = atomic.AddInt64(&invocationService.nextCorrelation, 1)
	return invocationService.nextCorrelation
}

func (invocationService *invocationService) invokeOnPartitionOwner(request *protocol.ClientMessage, partitionId int32) invocationResult {
	invocation := newInvocation(request, partitionId, nil, nil, invocationService.client)
	return invocationService.sendInvocation(invocation)
}

func (invocationService *invocationService) invokeOnRandomTarget(request *protocol.ClientMessage) invocationResult {
	invocation := newInvocation(request, -1, nil, nil, invocationService.client)
	return invocationService.sendInvocation(invocation)
}

func (invocationService *invocationService) invokeOnKeyOwner(request *protocol.ClientMessage, keyData *serialization.Data) invocationResult {
	partitionId := invocationService.client.PartitionService.GetPartitionId(keyData)
	return invocationService.invokeOnPartitionOwner(request, partitionId)
}

func (invocationService *invocationService) invokeOnTarget(request *protocol.ClientMessage, target *protocol.Address) invocationResult {
	invocation := newInvocation(request, -1, target, nil, invocationService.client)
	return invocationService.sendInvocation(invocation)
}

func (invocationService *invocationService) process() {
	for {
		select {
		case invocation := <-invocationService.sending:
			invocationService.registerInvocation(invocation)
			invocationService.invoke(invocation)
		case response := <-invocationService.responseChannel:
			invocationService.handleResponse(response)
		case correlationId := <-invocationService.notSentMessages:
			invocationService.handleNotSentInvocation(correlationId)
		case connectionAndErr := <-invocationService.cleanupConnectionChannel:
			invocationService.cleanupConnectionInternal(connectionAndErr.connection, connectionAndErr.error)
		case correlationId := <-invocationService.removeEventHandlerChannel:
			invocationService.removeEventHandlerInternal(correlationId)
		case correlationId := <-invocationService.unRegisterInvocationChannel:
			invocationService.unRegisterInvocation(correlationId)
		case invocationConnection := <-invocationService.sendToConnectionChannel:
			invocationService.sendToConnection(invocationConnection.invocation, invocationConnection.connection)
		case <-invocationService.quit:
			invocationService.quitInternal()
			return
		}
	}
}
func (invocationService *invocationService) quitInternal() {
	for _, invocation := range invocationService.responseWaitings {
		invocation.err <- core.NewHazelcastClientNotActiveError("client has been shutdown", nil)
	}
}
func (invocationService *invocationService) sendToRandomAddress(invocation *invocation) {
	var target *protocol.Address = invocationService.client.LoadBalancer.nextAddress()
	invocationService.sendToAddress(invocation, target)
}
func (invocationService *invocationService) invokeSmart(invocation *invocation) {
	if invocation.boundConnection != nil {
		invocationService.sendToConnectionChannel <- &invocationConnection{invocation: invocation, connection: invocation.boundConnection}
	} else if invocation.partitionId != -1 {
		if target, ok := invocationService.client.PartitionService.partitionOwner(invocation.partitionId); ok {
			invocationService.sendToAddress(invocation, target)
		} else {
			invocationService.handleException(invocation,
				core.NewHazelcastIOError(fmt.Sprintf("Partition does not have an owner. partitionId: %d", invocation.partitionId), nil))

		}
	} else if invocation.address != nil {
		invocationService.sendToAddress(invocation, invocation.address)
	} else {
		invocationService.sendToRandomAddress(invocation)
	}
}

func (invocationService *invocationService) invokeNonSmart(invocation *invocation) {
	if invocation.boundConnection != nil {
		invocationService.sendToConnectionChannel <- &invocationConnection{invocation: invocation, connection: invocation.boundConnection}
	} else {
		addr := invocationService.client.ClusterService.ownerConnectionAddress.Load().(*protocol.Address)
		invocationService.sendToAddress(invocation, addr)
	}
}

func (invocationService *invocationService) send(invocation *invocation, connectionChannel chan *Connection, errorChannel chan error) {
	go func() {
		select {
		case <-invocationService.quit:
			return
		case connection := <-connectionChannel:
			invocationService.sendToConnectionChannel <- &invocationConnection{invocation: invocation, connection: connection}
		case err := <-errorChannel:
			log.Println("the following error occured while trying to send the invocation: ", err)
			invocationService.handleException(invocation, err)
		}
	}()
}
func (invocationService *invocationService) sendInvocation(invocation *invocation) invocationResult {
	if invocationService.isShutdown.Load().(bool) {
		invocation.err <- core.NewHazelcastClientNotActiveError("client is shut down", nil)
		return invocation
	}
	invocationService.sending <- invocation
	return invocation
}
func (invocationService *invocationService) invokeOnConnection(request *protocol.ClientMessage, connection *Connection) invocationResult {
	invocation := newInvocation(request, -1, nil, connection, invocationService.client)
	return invocationService.sendInvocation(invocation)
}
func (invocationService *invocationService) sendToConnection(invocation *invocation, connection *Connection) {
	sent := connection.send(invocation.request)
	if !sent {
		//not sent
		invocationService.notSentMessages <- invocation.request.CorrelationId()
	} else {
		invocation.sentConnection = connection
		invocation.closed = connection.closed
	}

}

func (invocationService *invocationService) sendToAddress(invocation *invocation, address *protocol.Address) {
	connectionChannel, errorChannel := invocationService.client.ConnectionManager.getOrConnect(address, false)
	invocationService.send(invocation, connectionChannel, errorChannel)
}

func (invocationService *invocationService) registerInvocation(invocation *invocation) {
	message := invocation.request
	correlationId := invocationService.nextCorrelationId()
	message.SetCorrelationId(correlationId)
	message.SetPartitionId(invocation.partitionId)
	message.SetFlags(common.BeginEndFlag)
	if invocation.eventHandler != nil {
		invocationService.eventHandlers[correlationId] = invocation
	}
	invocationService.responseWaitings[correlationId] = invocation
}

func (invocationService *invocationService) unRegisterInvocation(correlationId int64) (*invocation, bool) {
	if invocation, ok := invocationService.responseWaitings[correlationId]; ok {
		defer delete(invocationService.responseWaitings, correlationId)
		return invocation, ok
	}
	if invocation, ok := invocationService.eventHandlers[correlationId]; ok {
		return invocation, ok
	}
	return nil, false
}

func (invocationService *invocationService) handleNotSentInvocation(correlationId int64) {
	if invocation, ok := invocationService.unRegisterInvocation(correlationId); ok {
		invocationService.handleException(invocation, core.NewHazelcastIOError("packet is not sent", nil))
	} else {
		log.Println("no invocation has been found with the correlation id: ", correlationId)
	}
}
func (invocationService *invocationService) removeEventHandler(correlationId int64) {
	invocationService.removeEventHandlerChannel <- correlationId
}
func (invocationService *invocationService) removeEventHandlerInternal(correlationId int64) {
	if _, ok := invocationService.eventHandlers[correlationId]; ok {
		delete(invocationService.eventHandlers, correlationId)
	}
}
func (invocationService *invocationService) handleResponse(response *protocol.ClientMessage) {
	correlationId := response.CorrelationId()
	if invocation, ok := invocationService.unRegisterInvocation(correlationId); ok {
		if response.HasFlags(common.ListenerFlag) > 0 {
			invocation, found := invocationService.eventHandlers[correlationId]
			if !found {
				log.Println("Got an event message with unknown correlation id")
			} else {
				invocation.eventHandler(response)
			}
			return
		}
		if response.MessageType() == common.MessageTypeException {
			err := CreateHazelcastError(convertToError(response))
			invocationService.handleException(invocation, err)
		} else {
			invocation.response <- response
		}
	} else {
		log.Println("no invocation has been found with the correlation id: ", correlationId)
	}
}

func convertToError(clientMessage *protocol.ClientMessage) *protocol.Error {
	return protocol.ErrorCodecDecode(clientMessage)
}
func (invocationService *invocationService) onConnectionClosed(connection *Connection, cause error) {
	invocationService.cleanupConnection(connection, cause)
}
func (invocationService *invocationService) onConnectionOpened(connection *Connection) {
}
func (invocationService *invocationService) cleanupConnection(connection *Connection, cause error) {
	invocationService.cleanupConnectionChannel <- &connectionAndError{connection: connection, error: cause}
}

func (invocationService *invocationService) cleanupConnectionInternal(connection *Connection, cause error) {
	for _, invocation := range invocationService.responseWaitings {
		if invocation.sentConnection == connection {
			invocationService.handleException(invocation, cause)
		}
	}

}
func (invocationService *invocationService) handleException(invocation *invocation, err error) {
	invocationService.unRegisterInvocationChannel <- invocation.request.CorrelationId()
	if !invocationService.client.LifecycleService.isLive.Load().(bool) {
		invocation.err <- core.NewHazelcastClientNotActiveError(err.Error(), err)
		return
	}
	if invocationService.isNotAllowedToRetryOnConnection(invocation, err) {
		invocation.err <- err
		return
	}

	if invocation.isTimedout.Load().(bool) {
		timeSinceDeadline := time.Since(invocation.timedoutTime.Load().(time.Time))
		log.Println("invocation will not be retried because it timed out by ", timeSinceDeadline.String())
		invocation.err <- core.NewHazelcastTimeoutError("invocation timed out by"+timeSinceDeadline.String(), nil)
		return
	}
	if invocationService.shouldRetryInvocation(invocation, err) {
		go func() {
			if invocationService.isShutdown.Load().(bool) {
				invocation.err <- core.NewHazelcastClientNotActiveError(err.Error(), err)
				return
			}
			time.Sleep(RetryWaitTimeInSeconds * time.Second)
			invocationService.sending <- invocation
			return
		}()
		return
	}
	invocation.err <- err
}

func (invocationService *invocationService) isRedoOperation() bool {
	return invocationService.client.ClientConfig.ClientNetworkConfig().IsRedoOperation()
}
func (invocationService *invocationService) shouldRetryInvocation(clientInvocation *invocation, err error) bool {
	_, isTargetDisconnectedError := err.(*core.HazelcastTargetDisconnectedError)
	if (isTargetDisconnectedError && clientInvocation.request.IsRetryable) || invocationService.isRedoOperation() || isRetrySafeError(err) {
		return true
	}
	return false
}
func isRetrySafeError(err error) bool {
	var isRetrySafe bool = false
	_, ok := err.(*core.HazelcastInstanceNotActiveError)
	isRetrySafe = isRetrySafe || ok
	_, ok = err.(*core.HazelcastTargetNotMemberError)
	isRetrySafe = isRetrySafe || ok
	_, ok = err.(*core.HazelcastIOError)
	isRetrySafe = isRetrySafe || ok
	return isRetrySafe
}
func (invocationService *invocationService) isNotAllowedToRetryOnConnection(invocation *invocation, err error) bool {
	_, isIOError := err.(*core.HazelcastIOError)
	if invocation.isBoundToSingleConnection() && isIOError {
		return true
	}
	_, isTargetNotMemberError := err.(*core.HazelcastTargetNotMemberError)
	if invocation.address != nil && isTargetNotMemberError && invocationService.client.ClusterService.GetMember(invocation.address) == nil {
		return true
	}
	return false
}
func (invocation *invocation) isBoundToSingleConnection() bool {
	return invocation.boundConnection != nil
}
func (invocationService *invocationService) shutdown() {
	invocationService.isShutdown.Store(true)
	close(invocationService.quit)
}
