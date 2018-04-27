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
	registrationIDToListenerRegistration       map[string]*listenerRegistrationKey
	failedRegistrations                        map[int64][]*listenerRegistrationKey
	register                                   chan *invocation
	registerListenerOnConnectionChannel        chan registrationIDConnection
	registerListenerOnConnectionErrChannel     chan error
	deregisterListenerChannel                  chan registrationIDRequestEncoder
	deregisterListenerErrChannel               chan removedErr
	onConnectionClosedChannel                  chan *Connection
	onConnectionOpenedChannel                  chan *Connection
	onHeartbeatRestoredChannel                 chan *Connection
	registerListenerInternalHandleErrorChannel chan registrationIDConnection
	registerListenerInitChannel                chan *listenerRegistrationKey
	connectToAllMembersChannel                 chan struct{}
}

type removedErr struct {
	removed bool
	err     error
}

type registrationIDRequestEncoder struct {
	registrationID string
	requestEncoder protocol.EncodeListenerRemoveRequest
}
type registrationIDConnection struct {
	registrationID string
	connection     *Connection
}

type eventRegistration struct {
	serverRegistrationID string
	correlationID        int64
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
		registrationIDToListenerRegistration:       make(map[string]*listenerRegistrationKey),
		failedRegistrations:                        make(map[int64][]*listenerRegistrationKey),
		registerListenerOnConnectionChannel:        make(chan registrationIDConnection, 1),
		registerListenerOnConnectionErrChannel:     make(chan error, 1),
		deregisterListenerChannel:                  make(chan registrationIDRequestEncoder, 1),
		deregisterListenerErrChannel:               make(chan removedErr, 1),
		onConnectionClosedChannel:                  make(chan *Connection, 1),
		onConnectionOpenedChannel:                  make(chan *Connection, 1),
		onHeartbeatRestoredChannel:                 make(chan *Connection, 1),
		registerListenerInternalHandleErrorChannel: make(chan registrationIDConnection, 1),
		registerListenerInitChannel:                make(chan *listenerRegistrationKey),
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
	for range ticker.C {
		ls.connectToAllMembersChannel <- struct{}{}
	}
}

func (ls *listenerService) process() {
	for {
		select {
		case registrationIDConnection := <-ls.registerListenerOnConnectionChannel:
			ls.registerListenerOnConnectionErrChannel <- ls.registerListenerOnConnection(registrationIDConnection.registrationID,
				registrationIDConnection.connection)
		case registrationIDRequestEncoder := <-ls.deregisterListenerChannel:
			removed, err := ls.deregisterListenerInternal(registrationIDRequestEncoder.registrationID,
				registrationIDRequestEncoder.requestEncoder)
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
		case registrationIDConnection := <-ls.registerListenerInternalHandleErrorChannel:
			ls.registerListenerFromInternalHandleError(registrationIDConnection.registrationID, registrationIDConnection.connection)
		case listenerRegistrationKey := <-ls.registerListenerInitChannel:
			ls.registerListenerInit(listenerRegistrationKey)
		case <-ls.connectToAllMembersChannel:
			ls.connectToAllMembersInternal()
		}
	}

}

func (ls *listenerService) registerListenerInit(key *listenerRegistrationKey) {
	ls.registrationIDToListenerRegistration[key.userRegistrationKey] = key
	ls.registrations[key.userRegistrationKey] = make(map[int64]*eventRegistration)
}

func (ls *listenerService) registerListener(request *protocol.ClientMessage,
	eventHandler func(clientMessage *protocol.ClientMessage),
	encodeListenerRemoveRequest protocol.EncodeListenerRemoveRequest,
	responseDecoder protocol.DecodeListenerResponse) (*string, error) {
	err := ls.trySyncConnectToAllConnections()
	if err != nil {
		return nil, err
	}
	userRegistrationID, _ := common.NewUUID()
	registrationKey := listenerRegistrationKey{
		userRegistrationKey: userRegistrationID,
		request:             request,
		responseDecoder:     responseDecoder,
		eventHandler:        eventHandler,
	}
	ls.registerListenerInitChannel <- &registrationKey
	connections := ls.client.ConnectionManager.getActiveConnections()
	for _, connection := range connections {
		registrationIDConnection := registrationIDConnection{
			registrationID: userRegistrationID,
			connection:     connection,
		}
		ls.registerListenerOnConnectionChannel <- registrationIDConnection
		err := <-ls.registerListenerOnConnectionErrChannel
		if err != nil {
			if connection.isAlive() {
				ls.deregisterListener(userRegistrationID, encodeListenerRemoveRequest)
				return nil, core.NewHazelcastErrorType("listener cannot be added", nil)
			}
		}
	}

	return &userRegistrationID, nil
}

func (ls *listenerService) registerListenerOnConnection(registrationID string, connection *Connection) error {
	if registrationMap, found := ls.registrations[registrationID]; found {
		_, found := registrationMap[connection.connectionID]
		if found {
			return nil
		}
	}
	registrationKey := ls.registrationIDToListenerRegistration[registrationID]
	invocation := newInvocation(registrationKey.request, -1, nil, connection, ls.client)
	invocation.eventHandler = registrationKey.eventHandler
	invocation.listenerResponseDecoder = registrationKey.responseDecoder
	responseMessage, err := ls.client.InvocationService.sendInvocation(invocation).Result()
	if err != nil {
		return err
	}
	serverRegistrationID := *registrationKey.responseDecoder(responseMessage)
	correlationID := registrationKey.request.CorrelationID()
	registration := &eventRegistration{
		serverRegistrationID: serverRegistrationID,
		correlationID:        correlationID,
		connection:           connection,
	}
	ls.registrations[registrationID][connection.connectionID] = registration
	return nil
}

func (ls *listenerService) deregisterListener(registrationID string,
	requestEncoder protocol.EncodeListenerRemoveRequest) (bool, error) {
	registrationIDRequestEncoder := registrationIDRequestEncoder{
		registrationID: registrationID,
		requestEncoder: requestEncoder,
	}
	ls.deregisterListenerChannel <- registrationIDRequestEncoder
	removedErr := <-ls.deregisterListenerErrChannel
	return removedErr.removed, removedErr.err
}

func (ls *listenerService) deregisterListenerInternal(registrationID string,
	requestEncoder protocol.EncodeListenerRemoveRequest) (bool, error) {
	var registrationMap map[int64]*eventRegistration
	var found bool
	if registrationMap, found = ls.registrations[registrationID]; !found {
		return false, nil
	}
	var successful = true
	var err error
	for _, registration := range registrationMap {
		connection := registration.connection
		serverRegistrationID := registration.serverRegistrationID
		request := requestEncoder(&serverRegistrationID)
		invocation := newInvocation(request, -1, nil, connection, ls.client)
		_, err = ls.client.InvocationService.sendInvocation(invocation).Result()
		if err != nil {
			if connection.isAlive() {
				successful = false
				log.Println("Deregistration of listener with ID ", registrationID, " has failed to address ",
					connection.endpoint.Load().(*protocol.Address).Host(),
					":", connection.endpoint.Load().(*protocol.Address).Port())
				continue
			}
		}
		ls.client.InvocationService.removeEventHandler(registration.correlationID)
		delete(registrationMap, connection.connectionID)
	}
	if successful {
		delete(ls.registrations, registrationID)
		delete(ls.registrationIDToListenerRegistration, registrationID)
	}
	return successful, err
}

func (ls *listenerService) registerListenerFromInternal(registrationID string, connection *Connection) {
	registrationIDConnection := registrationIDConnection{
		registrationID: registrationID,
		connection:     connection,
	}
	ls.registerListenerOnConnectionChannel <- registrationIDConnection
	err := <-ls.registerListenerOnConnectionErrChannel
	if err != nil {
		if _, ok := err.(*core.HazelcastIOError); ok {
			ls.registerListenerInternalHandleErrorChannel <- registrationIDConnection
		} else {
			log.Println("Listener ", registrationID, " cannot be added to a new Connection ", connection, ", reason :", err)
		}
	}

}

func (ls *listenerService) registerListenerFromInternalHandleError(registrationID string, connection *Connection) {
	failedRegsToConnection, found := ls.failedRegistrations[connection.connectionID]
	if !found {
		ls.failedRegistrations[connection.connectionID] = make([]*listenerRegistrationKey, 0)
	}
	registrationKey := ls.registrationIDToListenerRegistration[registrationID]
	ls.failedRegistrations[connection.connectionID] = append(failedRegsToConnection, registrationKey)
}

func (ls *listenerService) onConnectionClosed(connection *Connection, cause error) {
	ls.onConnectionClosedChannel <- connection
}

func (ls *listenerService) onConnectionClosedInternal(connection *Connection) {
	delete(ls.failedRegistrations, connection.connectionID)
	for _, registrationMap := range ls.registrations {
		registration, found := registrationMap[connection.connectionID]
		if found {
			delete(registrationMap, connection.connectionID)
			ls.client.InvocationService.removeEventHandler(registration.correlationID)
		}
	}
}

func (ls *listenerService) onConnectionOpened(connection *Connection) {
	ls.onConnectionOpenedChannel <- connection
}

func (ls *listenerService) onConnectionOpenedInternal(connection *Connection) {
	for registrationKey := range ls.registrations {
		go ls.registerListenerFromInternal(registrationKey, connection)
	}
}

func (ls *listenerService) OnHeartbeatRestored(connection *Connection) {
	ls.onHeartbeatRestoredChannel <- connection
}

func (ls *listenerService) OnHeartbeatRestoredInternal(connection *Connection) {
	registrationKeys := ls.failedRegistrations[connection.connectionID]
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
