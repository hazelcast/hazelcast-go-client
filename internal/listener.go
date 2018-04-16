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
func (ls *listenerService) connectToAllMembersInternal() {
	members := ls.client.ClusterService.GetMemberList()
	for _, member := range members {
		ls.client.ConnectionManager.getOrConnect(member.Address().(*protocol.Address), false)
	}
}
func (ls *listenerService) connectToAllMembersPeriodically() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			ls.connectToAllMembersChannel <- struct{}{}
		}
	}
}
func (ls *listenerService) process() {
	for {
		select {
		case registrationIdConnection := <-ls.registerListenerOnConnectionChannel:
			ls.registerListenerOnConnectionErrChannel <- ls.registerListenerOnConnection(registrationIdConnection.registrationId, registrationIdConnection.connection)
		case registrationIdRequestEncoder := <-ls.deregisterListenerChannel:
			removed, err := ls.deregisterListenerInternal(registrationIdRequestEncoder.registrationId, registrationIdRequestEncoder.requestEncoder)
			removedErr := removedErr{
				removed: removed,
				err:     err,
			}
			ls.deregisterListenerErrChannel <- removedErr
		case connection := <-ls.onConnectionClosedChannel:
			ls.onConnectionClosedInternal(connection)
		case connection := <-ls.onConnectionOpenedChannel:
			ls.onConnectionOpenedInternal(connection)
		case connection := <-ls.onHeartbeatRestoredChannel:
			ls.OnHeartbeatRestoredInternal(connection)
		case registrationIdConnection := <-ls.registerListenerInternalHandleErrorChannel:
			ls.registerListenerFromInternalHandleError(registrationIdConnection.registrationId, registrationIdConnection.connection)
		case listenerRegistrationKey := <-ls.registerListenerInitChannel:
			ls.registerListenerInit(listenerRegistrationKey)
		case <-ls.connectToAllMembersChannel:
			ls.connectToAllMembersInternal()
		}
	}

}

func (ls *listenerService) registerListenerInit(key *listenerRegistrationKey) {
	ls.registrationIdToListenerRegistration[key.userRegistrationKey] = key
	ls.registrations[key.userRegistrationKey] = make(map[int64]*eventRegistration)
}
func (ls *listenerService) registerListener(request *protocol.ClientMessage, eventHandler func(clientMessage *protocol.ClientMessage),
	encodeListenerRemoveRequest protocol.EncodeListenerRemoveRequest, responseDecoder protocol.DecodeListenerResponse) (*string, error) {
	err := ls.trySyncConnectToAllConnections()
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
	ls.registerListenerInitChannel <- &registrationKey
	connections := ls.client.ConnectionManager.getActiveConnections()
	for _, connection := range connections {
		registrationIdConnection := registrationIdConnection{
			registrationId: userRegistrationId,
			connection:     connection,
		}
		ls.registerListenerOnConnectionChannel <- registrationIdConnection
		err := <-ls.registerListenerOnConnectionErrChannel
		if err != nil {
			if connection.isAlive() {
				ls.deregisterListener(userRegistrationId, encodeListenerRemoveRequest)
				return nil, core.NewHazelcastErrorType("listener cannot be added", nil)
			}
		}
	}

	return &userRegistrationId, nil
}
func (ls *listenerService) registerListenerOnConnection(registrationId string, connection *Connection) error {
	if registrationMap, found := ls.registrations[registrationId]; found {
		_, found := registrationMap[connection.connectionId]
		if found {
			return nil
		}
	}
	registrationKey := ls.registrationIdToListenerRegistration[registrationId]
	invocation := newInvocation(registrationKey.request, -1, nil, connection, ls.client)
	invocation.eventHandler = registrationKey.eventHandler
	invocation.listenerResponseDecoder = registrationKey.responseDecoder
	responseMessage, err := ls.client.InvocationService.sendInvocation(invocation).Result()
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
	ls.registrations[registrationId][connection.connectionId] = registration
	return nil
}
func (ls *listenerService) deregisterListener(registrationId string, requestEncoder protocol.EncodeListenerRemoveRequest) (bool, error) {
	registrationIdRequestEncoder := registrationIdRequestEncoder{
		registrationId: registrationId,
		requestEncoder: requestEncoder,
	}
	ls.deregisterListenerChannel <- registrationIdRequestEncoder
	removedErr := <-ls.deregisterListenerErrChannel
	return removedErr.removed, removedErr.err
}
func (ls *listenerService) deregisterListenerInternal(registrationId string, requestEncoder protocol.EncodeListenerRemoveRequest) (bool, error) {
	var registrationMap map[int64]*eventRegistration
	var found bool
	if registrationMap, found = ls.registrations[registrationId]; !found {
		return false, nil
	}
	var successful bool = true
	var err error
	for _, registration := range registrationMap {
		connection := registration.connection
		serverRegistrationId := registration.serverRegistrationId
		request := requestEncoder(&serverRegistrationId)
		invocation := newInvocation(request, -1, nil, connection, ls.client)
		_, err = ls.client.InvocationService.sendInvocation(invocation).Result()
		if err != nil {
			if connection.isAlive() {
				successful = false
				log.Println("deregistration of listener with ID ", registrationId, " has failed to address ", connection.endpoint.Load().(*protocol.Address).Host(),
					":", connection.endpoint.Load().(*protocol.Address).Port())
				continue
			}
		}
		ls.client.InvocationService.removeEventHandler(registration.correlationId)
		delete(registrationMap, connection.connectionId)
	}
	if successful {
		delete(ls.registrations, registrationId)
		delete(ls.registrationIdToListenerRegistration, registrationId)
	}
	return successful, err
}

func (ls *listenerService) registerListenerFromInternal(registrationId string, connection *Connection) {
	registrationIdConnection := registrationIdConnection{
		registrationId: registrationId,
		connection:     connection,
	}
	ls.registerListenerOnConnectionChannel <- registrationIdConnection
	err := <-ls.registerListenerOnConnectionErrChannel
	if err != nil {
		if _, ok := err.(*core.HazelcastIOError); ok {
			ls.registerListenerInternalHandleErrorChannel <- registrationIdConnection
		} else {
			log.Println("listener ", registrationId, " cannot be added to a new Connection ", connection, ", reason :", err)
		}
	}

}
func (ls *listenerService) registerListenerFromInternalHandleError(registrationId string, connection *Connection) {
	failedRegsToConnection, found := ls.failedRegistrations[connection.connectionId]
	if !found {
		ls.failedRegistrations[connection.connectionId] = make([]*listenerRegistrationKey, 0)
	}
	registrationKey := ls.registrationIdToListenerRegistration[registrationId]
	ls.failedRegistrations[connection.connectionId] = append(failedRegsToConnection, registrationKey)
}
func (ls *listenerService) onConnectionClosed(connection *Connection, cause error) {
	ls.onConnectionClosedChannel <- connection
}
func (ls *listenerService) onConnectionClosedInternal(connection *Connection) {
	delete(ls.failedRegistrations, connection.connectionId)
	for _, registrationMap := range ls.registrations {
		registration, found := registrationMap[connection.connectionId]
		if found {
			delete(registrationMap, connection.connectionId)
			ls.client.InvocationService.removeEventHandler(registration.correlationId)
		}
	}
}
func (ls *listenerService) onConnectionOpened(connection *Connection) {
	ls.onConnectionOpenedChannel <- connection
}
func (ls *listenerService) onConnectionOpenedInternal(connection *Connection) {
	for registrationKey, _ := range ls.registrations {
		go ls.registerListenerFromInternal(registrationKey, connection)
	}
}
func (ls *listenerService) OnHeartbeatRestored(connection *Connection) {
	ls.onHeartbeatRestoredChannel <- connection
}
func (ls *listenerService) OnHeartbeatRestoredInternal(connection *Connection) {
	registrationKeys := ls.failedRegistrations[connection.connectionId]
	for _, registrationKey := range registrationKeys {
		go ls.registerListenerFromInternal(registrationKey.userRegistrationKey, connection)
	}
}
func (ls *listenerService) trySyncConnectToAllConnections() error {
	if !ls.client.ClientConfig.ClientNetworkConfig().IsSmartRouting() {
		return nil
	}
	remainingTime := ls.client.ClientConfig.ClientNetworkConfig().InvocationTimeout()
	for ls.client.LifecycleService.isLive.Load().(bool) && remainingTime > 0 {
		members := ls.client.GetCluster().GetMemberList()
		start := time.Now()
		successful := true
		for _, member := range members {
			connectionChannel, errorChannel := ls.client.ConnectionManager.getOrConnect(member.Address().(*protocol.Address), false)
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
