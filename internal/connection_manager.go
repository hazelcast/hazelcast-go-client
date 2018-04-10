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
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	AUTHENTICATED                  = iota
	CREDENTIALS_FAILED             = iota
	SERIALIZATION_VERSION_MISMATCH = iota
)

type connectionManager struct {
	client              *HazelcastClient
	connections         map[string]*Connection
	lock                sync.RWMutex
	nextConnectionId    int64
	connectionListeners atomic.Value
	mu                  sync.Mutex
}

func newConnectionManager(client *HazelcastClient) *connectionManager {
	cm := connectionManager{client: client,
		connections: make(map[string]*Connection),
	}
	cm.connectionListeners.Store(make([]connectionListener, 0)) //Initialize
	return &cm
}
func (connectionManager *connectionManager) nextConnectionID() int64 {
	return atomic.AddInt64(&connectionManager.nextConnectionId, 1)
}
func (connectionManager *connectionManager) addListener(listener connectionListener) {
	connectionManager.mu.Lock()
	defer connectionManager.mu.Unlock()
	if listener != nil {
		listeners := connectionManager.connectionListeners.Load().([]connectionListener)
		size := len(listeners) + 1
		copyListeners := make([]connectionListener, size)
		for index, listener := range listeners {
			copyListeners[index] = listener
		}
		copyListeners[size-1] = listener
		connectionManager.connectionListeners.Store(copyListeners)
	}
}
func (connectionManager *connectionManager) getActiveConnection(address *Address) *Connection {
	if address == nil {
		return nil
	}
	connectionManager.lock.RLock()
	if conn, found := connectionManager.connections[address.Host()+":"+strconv.Itoa(address.Port())]; found {
		connectionManager.lock.RUnlock()
		return conn
	}
	connectionManager.lock.RUnlock()
	return nil
}
func (connectionManager *connectionManager) getActiveConnections() map[string]*Connection {
	connections := make(map[string]*Connection)
	connectionManager.lock.RLock()
	defer connectionManager.lock.RUnlock()
	for k, v := range connectionManager.connections {
		connections[k] = v
	}
	return connections
}
func (connectionManager *connectionManager) connectionClosed(connection *Connection, cause error) {
	//If Connection was authenticated fire event
	if connection.endpoint.Load().(*Address).Host() != "" {
		connectionManager.lock.Lock()
		delete(connectionManager.connections, connection.endpoint.Load().(*Address).Host()+":"+strconv.Itoa(connection.endpoint.Load().(*Address).Port()))
		listeners := connectionManager.connectionListeners.Load().([]connectionListener)
		connectionManager.lock.Unlock()
		for _, listener := range listeners {
			if _, ok := listener.(connectionListener); ok {
				listener.(connectionListener).onConnectionClosed(connection, cause)
			}
		}
	} else {
		//Clean up unauthenticated Connection
		connectionManager.client.InvocationService.cleanupConnection(connection, cause)
	}
}
func (connectionManager *connectionManager) getOrConnect(address *Address, asOwner bool) (chan *Connection, chan error) {

	ch := make(chan *Connection, 1)
	err := make(chan error, 1)
	go func() {
		//First try readLock
		connectionManager.lock.RLock()
		if conn, found := connectionManager.connections[address.Host()+":"+strconv.Itoa(address.Port())]; found {
			ch <- conn
			connectionManager.lock.RUnlock()
			return
		}
		//Check if Connection is opened
		if conn, found := connectionManager.connections[address.Host()+":"+strconv.Itoa(address.Port())]; found {
			ch <- conn
			connectionManager.lock.RUnlock()
			return
		}
		connectionManager.lock.RUnlock()
		//Open new Connection
		error := connectionManager.openNewConnection(address, ch, asOwner)
		if error != nil {
			err <- error
		}
	}()
	return ch, err
}
func (connectionManager *connectionManager) ConnectionCount() int32 {
	connectionManager.lock.RLock()
	defer connectionManager.lock.RUnlock()
	return int32(len(connectionManager.connections))
}
func (connectionManager *connectionManager) openNewConnection(address *Address, resp chan *Connection, asOwner bool) error {
	if !asOwner && connectionManager.client.ClusterService.ownerConnectionAddress.Load().(*Address).Host() == "" {
		return core.NewHazelcastIllegalStateError("ownerConnection is not active", nil)
	}
	invocationService := connectionManager.client.InvocationService
	connectionId := connectionManager.nextConnectionID()
	con := newConnection(address, invocationService.responseChannel, invocationService.notSentMessages, connectionId, connectionManager)
	if con == nil {
		return core.NewHazelcastTargetDisconnectedError("target is disconnected", nil)
	}
	err := connectionManager.clusterAuthenticator(con, asOwner)

	if err != nil {
		return err
	}
	resp <- con
	return nil
}

func (connectionManager *connectionManager) getOwnerConnection() *Connection {
	ownerConnectionAddress := connectionManager.client.ClusterService.ownerConnectionAddress.Load().(*Address)
	if ownerConnectionAddress.Host() == "" {
		return nil
	}
	return connectionManager.getActiveConnection(ownerConnectionAddress)

}

func (connectionManager *connectionManager) clusterAuthenticator(connection *Connection, asOwner bool) error {
	uuid := connectionManager.client.ClusterService.uuid.Load().(string)
	ownerUuid := connectionManager.client.ClusterService.ownerUuid.Load().(string)
	clientType := CLIENT_TYPE
	name := connectionManager.client.ClientConfig.GroupConfig().Name()
	password := connectionManager.client.ClientConfig.GroupConfig().Password()
	clientVersion := "ALPHA" //TODO This should be replace with a build time version variable, BuildInfo etc.
	request := ClientAuthenticationEncodeRequest(
		&name,
		&password,
		&uuid,
		&ownerUuid,
		asOwner,
		&clientType,
		1,
		&clientVersion,
	)
	result, err := connectionManager.client.InvocationService.invokeOnConnection(request, connection).Result()
	if err != nil {
		return err
	} else {

		status, address, uuid, ownerUuid /*serializationVersion*/, _, serverHazelcastVersion /*clientUnregisteredMembers*/, _ := ClientAuthenticationDecodeResponse(result)()
		switch status {
		case AUTHENTICATED:
			connection.serverHazelcastVersion = serverHazelcastVersion
			connection.endpoint.Store(address)
			connection.isOwnerConnection = asOwner
			connectionManager.lock.Lock()
			connectionManager.connections[address.Host()+":"+strconv.Itoa(address.Port())] = connection
			connectionManager.lock.Unlock()
			connectionManager.fireConnectionAddedEvent(connection)
			if asOwner {
				connectionManager.client.ClusterService.ownerConnectionAddress.Store(connection.endpoint.Load().(*Address))
				connectionManager.client.ClusterService.ownerUuid.Store(*ownerUuid)
				connectionManager.client.ClusterService.uuid.Store(*uuid)
			}
		case CREDENTIALS_FAILED:
			return core.NewHazelcastAuthenticationError("invalid credentials!", nil)
		case SERIALIZATION_VERSION_MISMATCH:
			return core.NewHazelcastAuthenticationError("serialization version mismatches with the server!", nil)
		}
	}
	return nil
}
func (connectionManager *connectionManager) fireConnectionAddedEvent(connection *Connection) {
	listeners := connectionManager.connectionListeners.Load().([]connectionListener)
	for _, listener := range listeners {
		if _, ok := listener.(connectionListener); ok {
			listener.(connectionListener).onConnectionOpened(connection)
		}
	}
}

type connectionListener interface {
	onConnectionClosed(connection *Connection, cause error)
	onConnectionOpened(connection *Connection)
}
