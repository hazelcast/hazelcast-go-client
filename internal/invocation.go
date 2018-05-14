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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const retryWaitTimeInSeconds = 1

type invocation struct {
	boundConnection         *Connection
	sentConnection          *Connection
	address                 *proto.Address
	request                 *proto.ClientMessage
	partitionID             int32
	response                chan *proto.ClientMessage
	err                     chan error
	done                    chan bool
	eventHandler            func(clientMessage *proto.ClientMessage)
	timeout                 <-chan time.Time
	isTimedout              atomic.Value
	timedoutTime            atomic.Value
	listenerResponseDecoder proto.DecodeListenerResponse
}

type connectionAndError struct {
	connection *Connection
	error      error
}

type invocationResult interface {
	Result() (*proto.ClientMessage, error)
}

func newInvocation(request *proto.ClientMessage, partitionID int32, address *proto.Address,
	connection *Connection, client *HazelcastClient) *invocation {
	invocation := &invocation{
		request:         request,
		partitionID:     partitionID,
		address:         address,
		boundConnection: connection,
		response:        make(chan *proto.ClientMessage, 10),
		err:             make(chan error, 1),
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

func (i *invocation) Result() (*proto.ClientMessage, error) {
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
	responseChannel             chan *proto.ClientMessage
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
	service := &invocationService{client: client,
		sending:          make(chan *invocation, 10000),
		responseWaitings: make(map[int64]*invocation),
		eventHandlers:    make(map[int64]*invocation),
		responseChannel:  make(chan *proto.ClientMessage, 1),
		quit:             make(chan struct{}),
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

func (is *invocationService) nextCorrelationID() int64 {
	return atomic.AddInt64(&is.nextCorrelation, 1)
}

func (is *invocationService) invokeOnPartitionOwner(request *proto.ClientMessage, partitionID int32) invocationResult {
	invocation := newInvocation(request, partitionID, nil, nil, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationService) invokeOnRandomTarget(request *proto.ClientMessage) invocationResult {
	invocation := newInvocation(request, -1, nil, nil, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationService) invokeOnKeyOwner(request *proto.ClientMessage, keyData *serialization.Data) invocationResult {
	partitionID := is.client.PartitionService.GetPartitionID(keyData)
	return is.invokeOnPartitionOwner(request, partitionID)
}

func (is *invocationService) invokeOnTarget(request *proto.ClientMessage, target *proto.Address) invocationResult {
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
		case correlationID := <-is.notSentMessages:
			is.handleNotSentInvocation(correlationID)
		case connectionAndErr := <-is.cleanupConnectionChannel:
			is.cleanupConnectionInternal(connectionAndErr.connection, connectionAndErr.error)
		case correlationID := <-is.removeEventHandlerChannel:
			is.removeEventHandlerInternal(correlationID)
		case correlationID := <-is.unRegisterInvocationChannel:
			is.unRegisterInvocation(correlationID)
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
	var target = is.client.LoadBalancer.nextAddress()
	is.sendToAddress(invocation, target)
}

func (is *invocationService) invokeSmart(invocation *invocation) {
	if invocation.boundConnection != nil {
		is.sendToConnectionChannel <- &invocationConnection{invocation: invocation, connection: invocation.boundConnection}
	} else if invocation.partitionID != -1 {
		if target, ok := is.client.PartitionService.partitionOwner(invocation.partitionID); ok {
			is.sendToAddress(invocation, target)
		} else {
			is.handleError(invocation,
				core.NewHazelcastIOError(fmt.Sprintf("partition does not have an owner. partitionID: %d", invocation.partitionID), nil))
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
		addr := is.client.ClusterService.ownerConnectionAddress.Load().(*proto.Address)
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
			log.Println("The following error occurred while trying to send the invocation: ", err)
			is.handleError(invocation, err)
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

func (is *invocationService) invokeOnConnection(request *proto.ClientMessage, connection *Connection) invocationResult {
	invocation := newInvocation(request, -1, nil, connection, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationService) sendToConnection(invocation *invocation, connection *Connection) {
	sent := connection.send(invocation.request)
	if !sent {
		//not sent
		is.notSentMessages <- invocation.request.CorrelationID()
	} else {
		invocation.sentConnection = connection
	}

}

func (is *invocationService) sendToAddress(invocation *invocation, address *proto.Address) {
	connectionChannel, errorChannel := is.client.ConnectionManager.getOrConnect(address, false)
	is.send(invocation, connectionChannel, errorChannel)
}

func (is *invocationService) registerInvocation(invocation *invocation) {
	message := invocation.request
	correlationID := is.nextCorrelationID()
	message.SetCorrelationID(correlationID)
	message.SetPartitionID(invocation.partitionID)
	message.SetFlags(bufutil.BeginEndFlag)
	if invocation.eventHandler != nil {
		is.eventHandlers[correlationID] = invocation
	}
	is.responseWaitings[correlationID] = invocation
}

func (is *invocationService) unRegisterInvocation(correlationID int64) (*invocation, bool) {
	if invocation, ok := is.responseWaitings[correlationID]; ok {
		defer delete(is.responseWaitings, correlationID)
		return invocation, ok
	}
	if invocation, ok := is.eventHandlers[correlationID]; ok {
		return invocation, ok
	}
	return nil, false
}

func (is *invocationService) handleNotSentInvocation(correlationID int64) {
	if invocation, ok := is.unRegisterInvocation(correlationID); ok {
		is.handleError(invocation, core.NewHazelcastIOError("packet is not sent", nil))
	} else {
		log.Println("No invocation has been found with the correlation iD: ", correlationID)
	}
}

func (is *invocationService) removeEventHandler(correlationID int64) {
	is.removeEventHandlerChannel <- correlationID
}

func (is *invocationService) removeEventHandlerInternal(correlationID int64) {
	if _, ok := is.eventHandlers[correlationID]; ok {
		delete(is.eventHandlers, correlationID)
	}
}

func (is *invocationService) handleResponse(response *proto.ClientMessage) {
	correlationID := response.CorrelationID()
	if invocation, ok := is.unRegisterInvocation(correlationID); ok {
		if response.HasFlags(bufutil.ListenerFlag) > 0 {
			invocation, found := is.eventHandlers[correlationID]
			if !found {
				log.Println("Got an event message with unknown correlation id.")
			} else {
				invocation.eventHandler(response)
			}
			return
		}
		if response.MessageType() == bufutil.MessageTypeException {
			err := createHazelcastError(convertToError(response))
			is.handleError(invocation, err)
		} else {
			invocation.response <- response
		}
	} else {
		log.Println("No invocation has been found with the correlation iD: ", correlationID)
	}
}

func convertToError(clientMessage *proto.ClientMessage) *proto.Error {
	return proto.ErrorCodecDecode(clientMessage)
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
			is.handleError(invocation, cause)
		}
	}

}

func (is *invocationService) handleError(invocation *invocation, err error) {
	is.unRegisterInvocationChannel <- invocation.request.CorrelationID()
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
		log.Println("Invocation will not be retried because it timed out by ", timeSinceDeadline.String())
		invocation.err <- core.NewHazelcastTimeoutError("invocation timed out by"+timeSinceDeadline.String(), nil)
		return
	}
	if is.shouldRetryInvocation(invocation, err) {
		go func() {
			if is.isShutdown.Load().(bool) {
				invocation.err <- core.NewHazelcastClientNotActiveError(err.Error(), err)
				return
			}
			time.Sleep(retryWaitTimeInSeconds * time.Second)
			is.sending <- invocation
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
	var isRetrySafe = false
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
