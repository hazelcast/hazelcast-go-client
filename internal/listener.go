// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"log"
	"time"
)

type ListenerService struct {
	client                                     *HazelcastClient
	registrations                              map[string]map[Address]*eventRegistration
	registrationIdToListenerRegistration       map[string]*listenerRegistrationKey
	failedRegistrations                        map[Address][]*listenerRegistrationKey
	register                                   chan *Invocation
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
	requestEncoder EncodeListenerRemoveRequest
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
	request             *ClientMessage
	responseDecoder     DecodeListenerResponse
	eventHandler        func(clientMessage *ClientMessage)
}

func newListenerService(client *HazelcastClient) *ListenerService {
	service := &ListenerService{client: client, register: make(chan *Invocation, 1),

		registrations:                              make(map[string]map[Address]*eventRegistration),
		registrationIdToListenerRegistration:       make(map[string]*listenerRegistrationKey),
		failedRegistrations:                        make(map[Address][]*listenerRegistrationKey),
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
	service.client.ConnectionManager.AddListener(service)
	service.client.HeartBeatService.AddHeartbeatListener(service)
	go service.process()
	go service.connectToAllMembersPeriodically()
	return service
}
func (listenerService *ListenerService) connectToAllMembersInternal() {
	members := listenerService.client.ClusterService.GetMemberList()
	for _, member := range members {
		listenerService.client.ConnectionManager.GetOrConnect(member.Address().(*Address))
	}
}
func (listenerService *ListenerService) connectToAllMembersPeriodically() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			listenerService.connectToAllMembersChannel <- struct{}{}
		}
	}
}
func (listenerService *ListenerService) process() {
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

func (listenerService *ListenerService) registerListenerInit(key *listenerRegistrationKey) {
	listenerService.registrationIdToListenerRegistration[key.userRegistrationKey] = key
	listenerService.registrations[key.userRegistrationKey] = make(map[Address]*eventRegistration)
}
func (listenerService *ListenerService) registerListener(request *ClientMessage, eventHandler func(clientMessage *ClientMessage),
	encodeListenerRemoveRequest EncodeListenerRemoveRequest, responseDecoder DecodeListenerResponse) (*string, error) {
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
	connections := listenerService.client.ConnectionManager.getActiveConnections()
	listenerService.registerListenerInitChannel <- &registrationKey
	for _, connection := range connections {
		registrationIdConnection := registrationIdConnection{
			registrationId: userRegistrationId,
			connection:     connection,
		}
		listenerService.registerListenerOnConnectionChannel <- registrationIdConnection
		err := <-listenerService.registerListenerOnConnectionErrChannel
		if err != nil {
			if connection.IsAlive() {
				listenerService.deregisterListener(userRegistrationId, encodeListenerRemoveRequest)
				return nil, common.NewHazelcastErrorType("listener cannot be added", nil)
			}
		}
	}

	return &userRegistrationId, nil
}
func (listenerService *ListenerService) registerListenerOnConnection(registrationId string, connection *Connection) error {
	if registrationMap, found := listenerService.registrations[registrationId]; found {
		_, found := registrationMap[*connection.endpoint]
		if found {
			return nil
		}
	}
	registrationKey := listenerService.registrationIdToListenerRegistration[registrationId]
	invocation := NewInvocation(registrationKey.request, -1, nil, connection, listenerService.client)
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
	listenerService.registrations[registrationId][*connection.endpoint] = registration
	return nil
}
func (listenerService *ListenerService) deregisterListener(registrationId string, requestEncoder EncodeListenerRemoveRequest) (bool, error) {
	registrationIdRequestEncoder := registrationIdRequestEncoder{
		registrationId: registrationId,
		requestEncoder: requestEncoder,
	}
	listenerService.deregisterListenerChannel <- registrationIdRequestEncoder
	removedErr := <-listenerService.deregisterListenerErrChannel
	return removedErr.removed, removedErr.err
}
func (listenerService *ListenerService) deregisterListenerInternal(registrationId string, requestEncoder EncodeListenerRemoveRequest) (bool, error) {
	var registrationMap map[Address]*eventRegistration
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
		invocation := NewInvocation(request, -1, nil, connection, listenerService.client)
		_, err = listenerService.client.InvocationService.sendInvocation(invocation).Result()
		if err != nil {
			if connection.IsAlive() {
				successful = false
				log.Println("deregistration of listener with ID ", registrationId, " has failed to address ", connection.endpoint.Host(),
					":", connection.endpoint.Port())
				continue
			}
		}
		listenerService.client.InvocationService.removeEventHandler(registration.correlationId)
		delete(registrationMap, *connection.endpoint)
	}
	if successful {
		delete(listenerService.registrations, registrationId)
		delete(listenerService.registrationIdToListenerRegistration, registrationId)
	}
	return successful, err
}

func (listenerService *ListenerService) registerListenerFromInternal(registrationId string, connection *Connection) {
	registrationIdConnection := registrationIdConnection{
		registrationId: registrationId,
		connection:     connection,
	}
	listenerService.registerListenerOnConnectionChannel <- registrationIdConnection
	err := <-listenerService.registerListenerOnConnectionErrChannel
	if err != nil {
		if _, ok := err.(*common.HazelcastIOError); ok {
			listenerService.registerListenerInternalHandleErrorChannel <- registrationIdConnection
		} else {
			log.Println("listener ", registrationId, " cannot be added to a new connection ", connection, ", reason :", err)
		}
	}

}
func (listenerService *ListenerService) registerListenerFromInternalHandleError(registrationId string, connection *Connection) {
	failedRegsToConnection, found := listenerService.failedRegistrations[*connection.endpoint]
	if !found {
		listenerService.failedRegistrations[*connection.endpoint] = make([]*listenerRegistrationKey, 0)
	}
	registrationKey := listenerService.registrationIdToListenerRegistration[registrationId]
	listenerService.failedRegistrations[*connection.endpoint] = append(failedRegsToConnection, registrationKey)
}
func (listenerService *ListenerService) onConnectionClosed(connection *Connection, cause error) {
	listenerService.onConnectionClosedChannel <- connection
}
func (listenerService *ListenerService) onConnectionClosedInternal(connection *Connection) {
	delete(listenerService.failedRegistrations, *connection.endpoint)
	for _, registrationMap := range listenerService.registrations {
		registration, found := registrationMap[*connection.endpoint]
		if found {
			delete(registrationMap, *connection.endpoint)
			listenerService.client.InvocationService.removeEventHandler(registration.correlationId)
		}
	}
}
func (listenerService *ListenerService) onConnectionOpened(connection *Connection) {
	listenerService.onConnectionOpenedChannel <- connection
}
func (listenerService *ListenerService) onConnectionOpenedInternal(connection *Connection) {
	for registrationKey, _ := range listenerService.registrations {
		go listenerService.registerListenerFromInternal(registrationKey, connection)
	}
}
func (listenerService *ListenerService) OnHeartbeatRestored(connection *Connection) {
	listenerService.onHeartbeatRestoredChannel <- connection
}
func (listenerService *ListenerService) OnHeartbeatRestoredInternal(connection *Connection) {
	registrationKeys := listenerService.failedRegistrations[*connection.endpoint]
	for _, registrationKey := range registrationKeys {
		go listenerService.registerListenerFromInternal(registrationKey.userRegistrationKey, connection)
	}
}
func (listenerService *ListenerService) trySyncConnectToAllConnections() error {
	if !listenerService.client.ClientConfig.IsSmartRouting() {
		return nil
	}
	remainingTime := listenerService.client.ClientConfig.ClientNetworkConfig.InvocationTimeout()
	for listenerService.client.LifecycleService.isLive.Load().(bool) && remainingTime > 0 {
		members := listenerService.client.GetCluster().GetMemberList()
		start := time.Now()
		successful := true
		for _, member := range members {
			connectionChannel, errorChannel := listenerService.client.ConnectionManager.GetOrConnect(member.Address().(*Address), false)
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
	return common.NewHazelcastTimeoutError("registering listeners timed out.", nil)

}
