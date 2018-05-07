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
	authenticated = iota
	credentialsFailed
	serializationVersionMismatch
)

type connectionManager struct {
	client              *HazelcastClient
	connections         map[string]*Connection
	lock                sync.RWMutex
	nextConnID          int64
	connectionListeners atomic.Value
	listenerLock        sync.Mutex
}

func newConnectionManager(client *HazelcastClient) *connectionManager {
	cm := connectionManager{client: client,
		connections: make(map[string]*Connection),
	}
	cm.connectionListeners.Store(make([]connectionListener, 0)) //Initialize
	return &cm
}

func (cm *connectionManager) NextConnectionID() int64 {
	return atomic.AddInt64(&cm.nextConnID, 1)
}

func (cm *connectionManager) addListener(listener connectionListener) {
	cm.listenerLock.Lock()
	defer cm.listenerLock.Unlock()
	if listener != nil {
		listeners := cm.connectionListeners.Load().([]connectionListener)
		size := len(listeners) + 1
		copyListeners := make([]connectionListener, size)
		copy(copyListeners, listeners)
		copyListeners[size-1] = listener
		cm.connectionListeners.Store(copyListeners)
	}
}

func (cm *connectionManager) getActiveConnection(address core.Address) *Connection {
	if address == nil {
		return nil
	}
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	if conn, found := cm.connections[address.Host()+":"+strconv.Itoa(address.Port())]; found {
		return conn
	}
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
		delete(cm.connections, connection.endpoint.Load().(*protocol.Address).Host()+":"+
			strconv.Itoa(connection.endpoint.Load().(*protocol.Address).Port()))
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

func (cm *connectionManager) getOrConnect(address core.Address, asOwner bool) (chan *Connection, chan error) {
	ch := make(chan *Connection, 1)
	errCh := make(chan error, 1)
	go func() {
		//First tries to read under read lock
		conn := cm.getConnection(address, asOwner)
		if conn != nil {
			ch <- conn
			return
		}
		//if not available, checks and creates under write lock
		newConn, err := cm.getOrConnectInternal(address, asOwner)
		if err != nil {
			errCh <- err
		} else {
			ch <- newConn
		}
	}()
	return ch, errCh
}

func (cm *connectionManager) getConnection(address core.Address, asOwner bool) *Connection {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	return cm.getConnectionInternal(address, asOwner)
}

func (cm *connectionManager) getConnectionInternal(address core.Address, asOwner bool) *Connection {
	conn, found := cm.connections[address.Host()+":"+strconv.Itoa(address.Port())]
	if !found {
		return nil
	}

	if !asOwner {
		return conn
	}

	if conn.isOwnerConnection {
		return conn
	}

	return nil
}

func (cm *connectionManager) ConnectionCount() int32 {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	return int32(len(cm.connections))
}

func (cm *connectionManager) getOrConnectInternal(address core.Address, asOwner bool) (*Connection, error) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	conn := cm.getConnectionInternal(address, asOwner)
	if conn != nil {
		return conn, nil
	}

	if !asOwner && cm.client.ClusterService.ownerConnectionAddress.Load().(*protocol.Address).Host() == "" {
		return nil, core.NewHazelcastIllegalStateError("ownerConnection is not active", nil)
	}
	invocationService := cm.client.InvocationService
	connectionID := cm.NextConnectionID()
	con := newConnection(address, invocationService.responseChannel, invocationService.notSentMessages, connectionID, cm)
	if con == nil {
		return nil, core.NewHazelcastTargetDisconnectedError("target is disconnected", nil)
	}
	err := cm.authenticate(con, asOwner)

	if err != nil {
		return nil, err
	}

	return con, nil
}

func (cm *connectionManager) getOwnerConnection() *Connection {
	ownerConnectionAddress := cm.client.ClusterService.ownerConnectionAddress.Load().(*protocol.Address)
	if ownerConnectionAddress.Host() == "" {
		return nil
	}
	return cm.getActiveConnection(ownerConnectionAddress)

}

func (cm *connectionManager) authenticate(connection *Connection, asOwner bool) error {
	uuid := cm.client.ClusterService.uuid.Load().(string)
	ownerUUID := cm.client.ClusterService.ownerUUID.Load().(string)
	clientType := protocol.ClientType
	name := cm.client.ClientConfig.GroupConfig().Name()
	password := cm.client.ClientConfig.GroupConfig().Password()
	clientVersion := "ALPHA" //TODO This should be replace with a build time version variable, BuildInfo etc.
	request := protocol.ClientAuthenticationEncodeRequest(
		name,
		password,
		uuid,
		ownerUUID,
		asOwner,
		clientType,
		1,
		clientVersion,
	)
	result, err := cm.client.InvocationService.invokeOnConnection(request, connection).Result()
	if err != nil {
		return err
	}
	/*serializationVersion, clientUnregisteredMembers*/
	status, address, uuid, ownerUUID, _, serverHazelcastVersion, _ := protocol.ClientAuthenticationDecodeResponse(result)()
	switch status {
	case authenticated:
		connection.serverHazelcastVersion = serverHazelcastVersion
		connection.endpoint.Store(address)
		connection.isOwnerConnection = asOwner
		cm.connections[address.Host()+":"+strconv.Itoa(address.Port())] = connection
		cm.fireConnectionAddedEvent(connection)
		if asOwner {
			cm.client.ClusterService.ownerConnectionAddress.Store(connection.endpoint.Load().(*protocol.Address))
			cm.client.ClusterService.ownerUUID.Store(ownerUUID)
			cm.client.ClusterService.uuid.Store(uuid)
		}
	case credentialsFailed:
		return core.NewHazelcastAuthenticationError("invalid credentials!", nil)
	case serializationVersionMismatch:
		return core.NewHazelcastAuthenticationError("serialization version mismatches with the server!", nil)
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

func (cm *connectionManager) shutdown() {
	activeCons := cm.getActiveConnections()
	for _, con := range activeCons {
		con.close(core.NewHazelcastClientNotActiveError("client is shutting down", nil))
	}
}

type connectionListener interface {
	onConnectionClosed(connection *Connection, cause error)
	onConnectionOpened(connection *Connection)
}
