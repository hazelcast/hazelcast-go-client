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
	"log"
	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

type listenerService struct {
	client                                     *HazelcastClient
	registrations                              map[string]map[int64]*eventRegistration
	registrationIdToListenerRegistration       map[string]*listenerRegistrationKey
	failedRegistrations                        map[int64][]*listenerRegistrationKey
	register                                   chan *invocation
	registerListenerOnConnectionChannel        chan registrationIdConnection
	registerListenerOnConnectionErrChannel     chan error
	deregisterListenerChannel                  chan registrationIdRequestEncoder
	deregisterListenerErrChannel               chan removedErr
	onConnectionClosedChannel                  chan *Connection
	onConnectionOpenedChannel                  chan *Connection
	onHeartbeatRestoredChannel                 chan *Connection
	registerListenerInternalHandleErrorChannel chan registrationIdConnection
	registerListenerInitChannel                chan *listenerRegistrationKey
	connectToAllMembersChannel                 chan struct{}
}
type removedErr struct {
	removed bool
	err     error
}
type registrationIdRequestEncoder struct {
	registrationId string
	requestEncoder protocol.EncodeListenerRemoveRequest
}
type registrationIdConnection struct {
	registrationId string
	connection     *Connection
}
type eventRegistration struct {
	serverRegistrationId string
	correlationId        int64
	connection           *Connection
}
type listenerRegistrationKey struct {
	userRegistrationKey string
	request             *protocol.ClientMessage
	responseDecoder     protocol.DecodeListenerResponse
	eventHandler        func(clientMessage *protocol.ClientMessage)
}

func newListenerService(client *HazelcastClient) *listenerService {
	service := &listenerService{client: client, register: make(chan *invocation, 1),

		registrations:                              make(map[string]map[int64]*eventRegistration),
		registrationIdToListenerRegistration:       make(map[string]*listenerRegistrationKey),
		failedRegistrations:                        make(map[int64][]*listenerRegistrationKey),
		registerListenerOnConnectionChannel:        make(chan registrationIdConnection, 1),
		registerListenerOnConnectionErrChannel:     make(chan error, 1),
		deregisterListenerChannel:                  make(chan registrationIdRequestEncoder, 1),
		deregisterListenerErrChannel:               make(chan removedErr, 1),
		onConnectionClosedChannel:                  make(chan *Connection, 1),
		onConnectionOpenedChannel:                  make(chan *Connection, 1),
		onHeartbeatRestoredChannel:                 make(chan *Connection, 1),
		registerListenerInternalHandleErrorChannel: make(chan registrationIdConnection, 1),
		registerListenerInitChannel:                make(chan *listenerRegistrationKey, 0),
		connectToAllMembersChannel:                 make(chan struct{}, 1),
	}
	service.client.ConnectionManager.addListener(service)
	go service.process()
	if service.client.ClientConfig.ClientNetworkConfig().IsSmartRouting() {
		service.client.HeartBeatService.AddHeartbeatListener(service)
		go service.connectToAllMembersPeriodically()
	}
	return service
}
func (listenerService *listenerService) connectToAllMembersInternal() {
	members := listenerService.client.ClusterService.GetMemberList()
	for _, member := range members {
		listenerService.client.ConnectionManager.getOrConnect(member.Address().(*protocol.Address), false)
	}
}
func (listenerService *listenerService) connectToAllMembersPeriodically() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			listenerService.connectToAllMembersChannel <- struct{}{}
		}
	}
}
func (listenerService *listenerService) process() {
	for {
		select {
		case registrationIdConnection := <-listenerService.registerListenerOnConnectionChannel:
			listenerService.registerListenerOnConnectionErrChannel <- listenerService.registerListenerOnConnection(registrationIdConnection.registrationId, registrationIdConnection.connection)
		case registrationIdRequestEncoder := <-listenerService.deregisterListenerChannel:
			removed, err := listenerService.deregisterListenerInternal(registrationIdRequestEncoder.registrationId, registrationIdRequestEncoder.requestEncoder)
			removedErr := removedErr{
				removed: removed,
				err:     err,
			}
			listenerService.deregisterListenerErrChannel <- removedErr
		case connection := <-listenerService.onConnectionClosedChannel:
			listenerService.onConnectionClosedInternal(connection)
		case connection := <-listenerService.onConnectionOpenedChannel:
			listenerService.onConnectionOpenedInternal(connection)
		case connection := <-listenerService.onHeartbeatRestoredChannel:
			listenerService.OnHeartbeatRestoredInternal(connection)
		case registrationIdConnection := <-listenerService.registerListenerInternalHandleErrorChannel:
			listenerService.registerListenerFromInternalHandleError(registrationIdConnection.registrationId, registrationIdConnection.connection)
		case listenerRegistrationKey := <-listenerService.registerListenerInitChannel:
			listenerService.registerListenerInit(listenerRegistrationKey)
		case <-listenerService.connectToAllMembersChannel:
			listenerService.connectToAllMembersInternal()
		}
	}

}

func (listenerService *listenerService) registerListenerInit(key *listenerRegistrationKey) {
	listenerService.registrationIdToListenerRegistration[key.userRegistrationKey] = key
	listenerService.registrations[key.userRegistrationKey] = make(map[int64]*eventRegistration)
}
func (listenerService *listenerService) registerListener(request *protocol.ClientMessage, eventHandler func(clientMessage *protocol.ClientMessage),
	encodeListenerRemoveRequest protocol.EncodeListenerRemoveRequest, responseDecoder protocol.DecodeListenerResponse) (*string, error) {
	err := listenerService.trySyncConnectToAllConnections()
	if err != nil {
		return nil, err
	}
	userRegistrationId, _ := common.NewUUID()
	registrationKey := listenerRegistrationKey{
		userRegistrationKey: userRegistrationId,
		request:             request,
		responseDecoder:     responseDecoder,
		eventHandler:        eventHandler,
	}
	listenerService.registerListenerInitChannel <- &registrationKey
	connections := listenerService.client.ConnectionManager.getActiveConnections()
	for _, connection := range connections {
		registrationIdConnection := registrationIdConnection{
			registrationId: userRegistrationId,
			connection:     connection,
		}
		listenerService.registerListenerOnConnectionChannel <- registrationIdConnection
		err := <-listenerService.registerListenerOnConnectionErrChannel
		if err != nil {
			if connection.isAlive() {
				listenerService.deregisterListener(userRegistrationId, encodeListenerRemoveRequest)
				return nil, core.NewHazelcastErrorType("listener cannot be added", nil)
			}
		}
	}

	return &userRegistrationId, nil
}
func (listenerService *listenerService) registerListenerOnConnection(registrationId string, connection *Connection) error {
	if registrationMap, found := listenerService.registrations[registrationId]; found {
		_, found := registrationMap[connection.connectionId]
		if found {
			return nil
		}
	}
	registrationKey := listenerService.registrationIdToListenerRegistration[registrationId]
	invocation := newInvocation(registrationKey.request, -1, nil, connection, listenerService.client)
	invocation.eventHandler = registrationKey.eventHandler
	invocation.listenerResponseDecoder = registrationKey.responseDecoder
	responseMessage, err := listenerService.client.InvocationService.sendInvocation(invocation).Result()
	if err != nil {
		return err
	}
	serverRegistrationId := *registrationKey.responseDecoder(responseMessage)
	correlationId := registrationKey.request.CorrelationId()
	registration := &eventRegistration{
		serverRegistrationId: serverRegistrationId,
		correlationId:        correlationId,
		connection:           connection,
	}
	listenerService.registrations[registrationId][connection.connectionId] = registration
	return nil
}
func (listenerService *listenerService) deregisterListener(registrationId string, requestEncoder protocol.EncodeListenerRemoveRequest) (bool, error) {
	registrationIdRequestEncoder := registrationIdRequestEncoder{
		registrationId: registrationId,
		requestEncoder: requestEncoder,
	}
	listenerService.deregisterListenerChannel <- registrationIdRequestEncoder
	removedErr := <-listenerService.deregisterListenerErrChannel
	return removedErr.removed, removedErr.err
}
func (listenerService *listenerService) deregisterListenerInternal(registrationId string, requestEncoder protocol.EncodeListenerRemoveRequest) (bool, error) {
	var registrationMap map[int64]*eventRegistration
	var found bool
	if registrationMap, found = listenerService.registrations[registrationId]; !found {
		return false, nil
	}
	var successful bool = true
	var err error
	for _, registration := range registrationMap {
		connection := registration.connection
		serverRegistrationId := registration.serverRegistrationId
		request := requestEncoder(&serverRegistrationId)
		invocation := newInvocation(request, -1, nil, connection, listenerService.client)
		_, err = listenerService.client.InvocationService.sendInvocation(invocation).Result()
		if err != nil {
			if connection.isAlive() {
				successful = false
				log.Println("deregistration of listener with ID ", registrationId, " has failed to address ", connection.endpoint.Load().(*protocol.Address).Host(),
					":", connection.endpoint.Load().(*protocol.Address).Port())
				continue
			}
		}
		listenerService.client.InvocationService.removeEventHandler(registration.correlationId)
		delete(registrationMap, connection.connectionId)
	}
	if successful {
		delete(listenerService.registrations, registrationId)
		delete(listenerService.registrationIdToListenerRegistration, registrationId)
	}
	return successful, err
}

func (listenerService *listenerService) registerListenerFromInternal(registrationId string, connection *Connection) {
	registrationIdConnection := registrationIdConnection{
		registrationId: registrationId,
		connection:     connection,
	}
	listenerService.registerListenerOnConnectionChannel <- registrationIdConnection
	err := <-listenerService.registerListenerOnConnectionErrChannel
	if err != nil {
		if _, ok := err.(*core.HazelcastIOError); ok {
			listenerService.registerListenerInternalHandleErrorChannel <- registrationIdConnection
		} else {
			log.Println("listener ", registrationId, " cannot be added to a new Connection ", connection, ", reason :", err)
		}
	}

}
func (listenerService *listenerService) registerListenerFromInternalHandleError(registrationId string, connection *Connection) {
	failedRegsToConnection, found := listenerService.failedRegistrations[connection.connectionId]
	if !found {
		listenerService.failedRegistrations[connection.connectionId] = make([]*listenerRegistrationKey, 0)
	}
	registrationKey := listenerService.registrationIdToListenerRegistration[registrationId]
	listenerService.failedRegistrations[connection.connectionId] = append(failedRegsToConnection, registrationKey)
}
func (listenerService *listenerService) onConnectionClosed(connection *Connection, cause error) {
	listenerService.onConnectionClosedChannel <- connection
}
func (listenerService *listenerService) onConnectionClosedInternal(connection *Connection) {
	delete(listenerService.failedRegistrations, connection.connectionId)
	for _, registrationMap := range listenerService.registrations {
		registration, found := registrationMap[connection.connectionId]
		if found {
			delete(registrationMap, connection.connectionId)
			listenerService.client.InvocationService.removeEventHandler(registration.correlationId)
		}
	}
}
func (listenerService *listenerService) onConnectionOpened(connection *Connection) {
	listenerService.onConnectionOpenedChannel <- connection
}
func (listenerService *listenerService) onConnectionOpenedInternal(connection *Connection) {
	for registrationKey, _ := range listenerService.registrations {
		go listenerService.registerListenerFromInternal(registrationKey, connection)
	}
}
func (listenerService *listenerService) OnHeartbeatRestored(connection *Connection) {
	listenerService.onHeartbeatRestoredChannel <- connection
}
func (listenerService *listenerService) OnHeartbeatRestoredInternal(connection *Connection) {
	registrationKeys := listenerService.failedRegistrations[connection.connectionId]
	for _, registrationKey := range registrationKeys {
		go listenerService.registerListenerFromInternal(registrationKey.userRegistrationKey, connection)
	}
}
func (listenerService *listenerService) trySyncConnectToAllConnections() error {
	if !listenerService.client.ClientConfig.ClientNetworkConfig().IsSmartRouting() {
		return nil
	}
	remainingTime := listenerService.client.ClientConfig.ClientNetworkConfig().InvocationTimeout()
	for listenerService.client.LifecycleService.isLive.Load().(bool) && remainingTime > 0 {
		members := listenerService.client.GetCluster().GetMemberList()
		start := time.Now()
		successful := true
		for _, member := range members {
			connectionChannel, errorChannel := listenerService.client.ConnectionManager.getOrConnect(member.Address().(*protocol.Address), false)
			select {
			case <-connectionChannel:
			case <-errorChannel:
				successful = false
			}
		}
		if successful {
			return nil
		}
		timeSinceStart := time.Since(start)
		remainingTime = remainingTime - timeSinceStart
	}
	return core.NewHazelcastTimeoutError("registering listeners timed out.", nil)

}
