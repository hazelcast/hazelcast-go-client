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
	"log"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
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

func (i *invocation) Result() (*protocol.ClientMessage, error) {
	select {
	case response := <-i.response:
		i.done <- true
		return response, nil
	case err := <-i.err:
		i.done <- true
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
func (is *invocationService) start() {
	go is.process()
}

func (is *invocationService) nextCorrelationId() int64 {
	is.nextCorrelation = atomic.AddInt64(&is.nextCorrelation, 1)
	return is.nextCorrelation
}

func (is *invocationService) invokeOnPartitionOwner(request *protocol.ClientMessage, partitionId int32) invocationResult {
	invocation := newInvocation(request, partitionId, nil, nil, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationService) invokeOnRandomTarget(request *protocol.ClientMessage) invocationResult {
	invocation := newInvocation(request, -1, nil, nil, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationService) invokeOnKeyOwner(request *protocol.ClientMessage, keyData *serialization.Data) invocationResult {
	partitionId := is.client.PartitionService.GetPartitionId(keyData)
	return is.invokeOnPartitionOwner(request, partitionId)
}

func (is *invocationService) invokeOnTarget(request *protocol.ClientMessage, target *protocol.Address) invocationResult {
	invocation := newInvocation(request, -1, target, nil, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationService) process() {
	for {
		select {
		case invocation := <-is.sending:
			is.registerInvocation(invocation)
			is.invoke(invocation)
		case response := <-is.responseChannel:
			is.handleResponse(response)
		case correlationId := <-is.notSentMessages:
			is.handleNotSentInvocation(correlationId)
		case connectionAndErr := <-is.cleanupConnectionChannel:
			is.cleanupConnectionInternal(connectionAndErr.connection, connectionAndErr.error)
		case correlationId := <-is.removeEventHandlerChannel:
			is.removeEventHandlerInternal(correlationId)
		case correlationId := <-is.unRegisterInvocationChannel:
			is.unRegisterInvocation(correlationId)
		case invocationConnection := <-is.sendToConnectionChannel:
			is.sendToConnection(invocationConnection.invocation, invocationConnection.connection)
		case <-is.quit:
			is.quitInternal()
			return
		}
	}
}
func (is *invocationService) quitInternal() {
	for _, invocation := range is.responseWaitings {
		invocation.err <- core.NewHazelcastClientNotActiveError("client has been shutdown", nil)
	}
}
func (is *invocationService) sendToRandomAddress(invocation *invocation) {
	var target *protocol.Address = is.client.LoadBalancer.nextAddress()
	is.sendToAddress(invocation, target)
}
func (is *invocationService) invokeSmart(invocation *invocation) {
	if invocation.boundConnection != nil {
		is.sendToConnectionChannel <- &invocationConnection{invocation: invocation, connection: invocation.boundConnection}
	} else if invocation.partitionId != -1 {
		if target, ok := is.client.PartitionService.partitionOwner(invocation.partitionId); ok {
			is.sendToAddress(invocation, target)
		} else {
			is.handleException(invocation,
				core.NewHazelcastIOError(fmt.Sprintf("Partition does not have an owner. partitionId: %d", invocation.partitionId), nil))

		}
	} else if invocation.address != nil {
		is.sendToAddress(invocation, invocation.address)
	} else {
		is.sendToRandomAddress(invocation)
	}
}

func (is *invocationService) invokeNonSmart(invocation *invocation) {
	if invocation.boundConnection != nil {
		is.sendToConnectionChannel <- &invocationConnection{invocation: invocation, connection: invocation.boundConnection}
	} else {
		addr := is.client.ClusterService.ownerConnectionAddress.Load().(*protocol.Address)
		is.sendToAddress(invocation, addr)
	}
}

func (is *invocationService) send(invocation *invocation, connectionChannel chan *Connection, errorChannel chan error) {
	go func() {
		select {
		case <-is.quit:
			return
		case connection := <-connectionChannel:
			is.sendToConnectionChannel <- &invocationConnection{invocation: invocation, connection: connection}
		case err := <-errorChannel:
			log.Println("the following error occured while trying to send the invocation: ", err)
			is.handleException(invocation, err)
		}
	}()
}
func (is *invocationService) sendInvocation(invocation *invocation) invocationResult {
	if is.isShutdown.Load().(bool) {
		invocation.err <- core.NewHazelcastClientNotActiveError("client is shut down", nil)
		return invocation
	}
	is.sending <- invocation
	return invocation
}
func (is *invocationService) invokeOnConnection(request *protocol.ClientMessage, connection *Connection) invocationResult {
	invocation := newInvocation(request, -1, nil, connection, is.client)
	return is.sendInvocation(invocation)
}
func (is *invocationService) sendToConnection(invocation *invocation, connection *Connection) {
	sent := connection.send(invocation.request)
	if !sent {
		//not sent
		is.notSentMessages <- invocation.request.CorrelationId()
	} else {
		invocation.sentConnection = connection
		invocation.closed = connection.closed
	}

}

func (is *invocationService) sendToAddress(invocation *invocation, address *protocol.Address) {
	connectionChannel, errorChannel := is.client.ConnectionManager.getOrConnect(address, false)
	is.send(invocation, connectionChannel, errorChannel)
}

func (is *invocationService) registerInvocation(invocation *invocation) {
	message := invocation.request
	correlationId := is.nextCorrelationId()
	message.SetCorrelationId(correlationId)
	message.SetPartitionId(invocation.partitionId)
	message.SetFlags(common.BeginEndFlag)
	if invocation.eventHandler != nil {
		is.eventHandlers[correlationId] = invocation
	}
	is.responseWaitings[correlationId] = invocation
}

func (is *invocationService) unRegisterInvocation(correlationId int64) (*invocation, bool) {
	if invocation, ok := is.responseWaitings[correlationId]; ok {
		defer delete(is.responseWaitings, correlationId)
		return invocation, ok
	}
	if invocation, ok := is.eventHandlers[correlationId]; ok {
		return invocation, ok
	}
	return nil, false
}

func (is *invocationService) handleNotSentInvocation(correlationId int64) {
	if invocation, ok := is.unRegisterInvocation(correlationId); ok {
		is.handleException(invocation, core.NewHazelcastIOError("packet is not sent", nil))
	} else {
		log.Println("no invocation has been found with the correlation id: ", correlationId)
	}
}
func (is *invocationService) removeEventHandler(correlationId int64) {
	is.removeEventHandlerChannel <- correlationId
}
func (is *invocationService) removeEventHandlerInternal(correlationId int64) {
	if _, ok := is.eventHandlers[correlationId]; ok {
		delete(is.eventHandlers, correlationId)
	}
}
func (is *invocationService) handleResponse(response *protocol.ClientMessage) {
	correlationId := response.CorrelationId()
	if invocation, ok := is.unRegisterInvocation(correlationId); ok {
		if response.HasFlags(common.ListenerFlag) > 0 {
			invocation, found := is.eventHandlers[correlationId]
			if !found {
				log.Println("Got an event message with unknown correlation id")
			} else {
				invocation.eventHandler(response)
			}
			return
		}
		if response.MessageType() == common.MessageTypeException {
			err := CreateHazelcastError(convertToError(response))
			is.handleException(invocation, err)
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
func (is *invocationService) onConnectionClosed(connection *Connection, cause error) {
	is.cleanupConnection(connection, cause)
}
func (is *invocationService) onConnectionOpened(connection *Connection) {
}
func (is *invocationService) cleanupConnection(connection *Connection, cause error) {
	is.cleanupConnectionChannel <- &connectionAndError{connection: connection, error: cause}
}

func (is *invocationService) cleanupConnectionInternal(connection *Connection, cause error) {
	for _, invocation := range is.responseWaitings {
		if invocation.sentConnection == connection {
			is.handleException(invocation, cause)
		}
	}

}
func (is *invocationService) handleException(invocation *invocation, err error) {
	is.unRegisterInvocationChannel <- invocation.request.CorrelationId()
	if !is.client.LifecycleService.isLive.Load().(bool) {
		invocation.err <- core.NewHazelcastClientNotActiveError(err.Error(), err)
		return
	}
	if is.isNotAllowedToRetryOnConnection(invocation, err) {
		invocation.err <- err
		return
	}

	if invocation.isTimedout.Load().(bool) {
		timeSinceDeadline := time.Since(invocation.timedoutTime.Load().(time.Time))
		log.Println("invocation will not be retried because it timed out by ", timeSinceDeadline.String())
		invocation.err <- core.NewHazelcastTimeoutError("invocation timed out by"+timeSinceDeadline.String(), nil)
		return
	}
	if is.shouldRetryInvocation(invocation, err) {
		go func() {
			if is.isShutdown.Load().(bool) {
				invocation.err <- core.NewHazelcastClientNotActiveError(err.Error(), err)
				return
			}
			time.Sleep(RetryWaitTimeInSeconds * time.Second)
			is.sending <- invocation
			return
		}()
		return
	}
	invocation.err <- err
}

func (is *invocationService) isRedoOperation() bool {
	return is.client.ClientConfig.ClientNetworkConfig().IsRedoOperation()
}
func (is *invocationService) shouldRetryInvocation(clientInvocation *invocation, err error) bool {
	_, isTargetDisconnectedError := err.(*core.HazelcastTargetDisconnectedError)
	if (isTargetDisconnectedError && clientInvocation.request.IsRetryable) || is.isRedoOperation() || isRetrySafeError(err) {
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
func (is *invocationService) isNotAllowedToRetryOnConnection(invocation *invocation, err error) bool {
	_, isIOError := err.(*core.HazelcastIOError)
	if invocation.isBoundToSingleConnection() && isIOError {
		return true
	}
	_, isTargetNotMemberError := err.(*core.HazelcastTargetNotMemberError)
	if invocation.address != nil && isTargetNotMemberError && is.client.ClusterService.GetMember(invocation.address) == nil {
		return true
	}
	return false
}
func (i *invocation) isBoundToSingleConnection() bool {
	return i.boundConnection != nil
}
func (is *invocationService) shutdown() {
	is.isShutdown.Store(true)
	close(is.quit)
}
