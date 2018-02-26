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
const (
	SERIALIZATION_VERSION uint8 = 1
)

var (
	SERVER_VERSION = "3.9"
)

type ConnectionManager struct {
	client              *HazelcastClient
	connections         map[string]*Connection
	lock                sync.RWMutex
	nextConnectionId    int64
	connectionListeners atomic.Value
	mu                  sync.Mutex
}

func NewConnectionManager(client *HazelcastClient) *ConnectionManager {
	cm := ConnectionManager{client: client,
		connections: make(map[string]*Connection),
	}
	cm.connectionListeners.Store(make([]connectionListener, 0)) //Initialize
	return &cm
}
func (connectionManager *ConnectionManager) NextConnectionId() int64 {
	connectionManager.nextConnectionId = atomic.AddInt64(&connectionManager.nextConnectionId, 1)
	return connectionManager.nextConnectionId
}
func (connectionManager *ConnectionManager) AddListener(listener connectionListener) {
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
func (connectionManager *ConnectionManager) getActiveConnection(address *Address) *Connection {
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
func (connectionManager *ConnectionManager) getActiveConnections() map[string]*Connection {
	connections := make(map[string]*Connection)
	connectionManager.lock.RLock()
	defer connectionManager.lock.RUnlock()
	for k, v := range connectionManager.connections {
		connections[k] = v
	}
	return connections
}
func (connectionManager *ConnectionManager) connectionClosed(connection *Connection, cause error) {
	//If connection was authenticated fire event
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
		//Clean up unauthenticated connection
		connectionManager.client.InvocationService.cleanupConnection(connection, cause)
	}
}
func (connectionManager *ConnectionManager) GetOrConnect(address *Address, asOwner bool) (chan *Connection, chan error) {

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
		connectionManager.lock.RUnlock()
		connectionManager.lock.Lock()
		defer connectionManager.lock.Unlock()
		//Check if connection is opened
		if conn, found := connectionManager.connections[address.Host()+":"+strconv.Itoa(address.Port())]; found {
			ch <- conn
			return
		}
		//Open new connection
		error := connectionManager.openNewConnection(address, ch, asOwner)
		if error != nil {
			err <- error
		}
	}()
	return ch, err
}
func (connectionManager *ConnectionManager) ConnectionCount() int32 {
	connectionManager.lock.RLock()
	defer connectionManager.lock.RUnlock()
	return int32(len(connectionManager.connections))
}
func (connectionManager *ConnectionManager) openNewConnection(address *Address, resp chan *Connection, asOwner bool) error {
	if !asOwner && connectionManager.client.ClusterService.ownerConnectionAddress.Load().(*Address).Host() == "" {
		return core.NewHazelcastIllegalStateError("ownerConnection is not active", nil)
	}
	invocationService := connectionManager.client.InvocationService
	connectionId := connectionManager.NextConnectionId()
	con := NewConnection(address, invocationService.responseChannel, invocationService.notSentMessages, connectionId, connectionManager)
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
func (connectionManager *ConnectionManager) getOwnerConnection() *Connection {
	ownerConnectionAddress := connectionManager.client.ClusterService.ownerConnectionAddress.Load().(*Address)
	if ownerConnectionAddress.Host() == "" {
		return nil
	}
	return connectionManager.getActiveConnection(ownerConnectionAddress)

}
func (connectionManager *ConnectionManager) clusterAuthenticator(connection *Connection, asOwner bool) error {
	uuid := connectionManager.client.ClusterService.uuid.Load().(string)
	ownerUuid := connectionManager.client.ClusterService.ownerUuid.Load().(string)
	clientType := CLIENT_TYPE
	name := connectionManager.client.ClientConfig.GroupConfig().Name()
	password := connectionManager.client.ClientConfig.GroupConfig().Password()
	request := ClientAuthenticationCodec.EncodeRequest(
		&name,
		&password,
		&uuid,
		&ownerUuid,
		asOwner,
		&clientType,
		SERIALIZATION_VERSION,
		&SERVER_VERSION,
	)
	result, err := connectionManager.client.InvocationService.InvokeOnConnection(request, connection).Result()
	if err != nil {
		return err
	} else {
		res, _ := ClientAuthenticationCodec.DecodeResponse(result, nil)
		parameters := res.(*ClientAuthenticationResponseParameters)
		switch parameters.Status {
		case AUTHENTICATED:
			connection.serverHazelcastVersion = parameters.ServerHazelcastVersion
			connection.endpoint.Store(parameters.Address)
			connection.isOwnerConnection = asOwner
			connectionManager.connections[parameters.Address.Host()+":"+strconv.Itoa(parameters.Address.Port())] = connection
			connectionManager.fireConnectionAddedEvent(connection)
			if asOwner {
				connectionManager.client.ClusterService.ownerConnectionAddress.Store(connection.endpoint.Load().(*Address))
				connectionManager.client.ClusterService.ownerUuid.Store(*parameters.OwnerUuid)
				connectionManager.client.ClusterService.uuid.Store(*parameters.Uuid)
			}
		case CREDENTIALS_FAILED:
			return core.NewHazelcastAuthenticationError("invalid credentials!", nil)
		case SERIALIZATION_VERSION_MISMATCH:
			return core.NewHazelcastAuthenticationError("serialization version mismatches with the server!", nil)
		}
	}
	return nil
}
func (connectionManager *ConnectionManager) fireConnectionAddedEvent(connection *Connection) {
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
