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
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

const (
	Authenticated                = iota
	CredentialsFailed            = iota
	SerializationVersionMismatch = iota
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

func (cm *connectionManager) nextConnectionID() int64 {
	return atomic.AddInt64(&cm.nextConnectionId, 1)
}

func (cm *connectionManager) addListener(listener connectionListener) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if listener != nil {
		listeners := cm.connectionListeners.Load().([]connectionListener)
		size := len(listeners) + 1
		copyListeners := make([]connectionListener, size)
		for index, listener := range listeners {
			copyListeners[index] = listener
		}
		copyListeners[size-1] = listener
		cm.connectionListeners.Store(copyListeners)
	}
}

func (cm *connectionManager) getActiveConnection(address *protocol.Address) *Connection {
	if address == nil {
		return nil
	}
	cm.lock.RLock()
	if conn, found := cm.connections[address.Host()+":"+strconv.Itoa(address.Port())]; found {
		cm.lock.RUnlock()
		return conn
	}
	cm.lock.RUnlock()
	return nil
}

func (cm *connectionManager) getActiveConnections() map[string]*Connection {
	connections := make(map[string]*Connection)
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	for k, v := range cm.connections {
		connections[k] = v
	}
	return connections
}

func (cm *connectionManager) connectionClosed(connection *Connection, cause error) {
	//If Connection was authenticated fire event
	if connection.endpoint.Load().(*protocol.Address).Host() != "" {
		cm.lock.Lock()
		delete(cm.connections, connection.endpoint.Load().(*protocol.Address).Host()+":"+strconv.Itoa(connection.endpoint.Load().(*protocol.Address).Port()))
		listeners := cm.connectionListeners.Load().([]connectionListener)
		cm.lock.Unlock()
		for _, listener := range listeners {
			if _, ok := listener.(connectionListener); ok {
				listener.(connectionListener).onConnectionClosed(connection, cause)
			}
		}
	} else {
		//Clean up unauthenticated Connection
		cm.client.InvocationService.cleanupConnection(connection, cause)
	}
}

func (cm *connectionManager) getOrConnect(address *protocol.Address, asOwner bool) (chan *Connection, chan error) {

	ch := make(chan *Connection, 1)
	err := make(chan error, 1)
	go func() {
		//First try readLock
		cm.lock.RLock()
		if conn, found := cm.connections[address.Host()+":"+strconv.Itoa(address.Port())]; found {
			ch <- conn
			cm.lock.RUnlock()
			return
		}
		//Check if Connection is opened
		if conn, found := cm.connections[address.Host()+":"+strconv.Itoa(address.Port())]; found {
			ch <- conn
			cm.lock.RUnlock()
			return
		}
		cm.lock.RUnlock()
		//Open new Connection
		error := cm.openNewConnection(address, ch, asOwner)
		if error != nil {
			err <- error
		}
	}()
	return ch, err
}

func (cm *connectionManager) ConnectionCount() int32 {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	return int32(len(cm.connections))
}

func (cm *connectionManager) openNewConnection(address *protocol.Address, resp chan *Connection, asOwner bool) error {
	if !asOwner && cm.client.ClusterService.ownerConnectionAddress.Load().(*protocol.Address).Host() == "" {
		return core.NewHazelcastIllegalStateError("ownerConnection is not active", nil)
	}
	invocationService := cm.client.InvocationService
	connectionId := cm.nextConnectionID()
	con := newConnection(address, invocationService.responseChannel, invocationService.notSentMessages, connectionId, cm)
	if con == nil {
		return core.NewHazelcastTargetDisconnectedError("target is disconnected", nil)
	}
	err := cm.clusterAuthenticator(con, asOwner)

	if err != nil {
		return err
	}
	resp <- con
	return nil
}

func (cm *connectionManager) getOwnerConnection() *Connection {
	ownerConnectionAddress := cm.client.ClusterService.ownerConnectionAddress.Load().(*protocol.Address)
	if ownerConnectionAddress.Host() == "" {
		return nil
	}
	return cm.getActiveConnection(ownerConnectionAddress)

}

func (cm *connectionManager) clusterAuthenticator(connection *Connection, asOwner bool) error {
	uuid := cm.client.ClusterService.uuid.Load().(string)
	ownerUuid := cm.client.ClusterService.ownerUuid.Load().(string)
	clientType := protocol.ClientType
	name := cm.client.ClientConfig.GroupConfig().Name()
	password := cm.client.ClientConfig.GroupConfig().Password()
	clientVersion := "ALPHA" //TODO This should be replace with a build time version variable, BuildInfo etc.
	request := protocol.ClientAuthenticationEncodeRequest(
		&name,
		&password,
		&uuid,
		&ownerUuid,
		asOwner,
		&clientType,
		1,
		&clientVersion,
	)
	result, err := cm.client.InvocationService.invokeOnConnection(request, connection).Result()
	if err != nil {
		return err
	} else {

		status, address, uuid, ownerUuid /*serializationVersion*/, _, serverHazelcastVersion /*clientUnregisteredMembers*/, _ := protocol.ClientAuthenticationDecodeResponse(result)()
		switch status {
		case Authenticated:
			connection.serverHazelcastVersion = serverHazelcastVersion
			connection.endpoint.Store(address)
			connection.isOwnerConnection = asOwner
			cm.lock.Lock()
			cm.connections[address.Host()+":"+strconv.Itoa(address.Port())] = connection
			cm.lock.Unlock()
			cm.fireConnectionAddedEvent(connection)
			if asOwner {
				cm.client.ClusterService.ownerConnectionAddress.Store(connection.endpoint.Load().(*protocol.Address))
				cm.client.ClusterService.ownerUuid.Store(*ownerUuid)
				cm.client.ClusterService.uuid.Store(*uuid)
			}
		case CredentialsFailed:
			return core.NewHazelcastAuthenticationError("invalid credentials!", nil)
		case SerializationVersionMismatch:
			return core.NewHazelcastAuthenticationError("serialization version mismatches with the server!", nil)
		}
	}
	return nil
}

func (cm *connectionManager) fireConnectionAddedEvent(connection *Connection) {
	listeners := cm.connectionListeners.Load().([]connectionListener)
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
