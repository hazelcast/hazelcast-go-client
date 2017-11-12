package internal

import (
	"github.com/hazelcast/go-client/core"
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"strconv"
	"sync"
	"sync/atomic"
)

type ConnectionManager struct {
	client              *HazelcastClient
	connections         map[string]*Connection
	ownerAddress        *Address
	lock                sync.RWMutex
	connectionListeners atomic.Value
	mu                  sync.Mutex
	nextConnectionId    int64
}

func NewConnectionManager(client *HazelcastClient) *ConnectionManager {
	cm := ConnectionManager{client: client,
		connections: make(map[string]*Connection),
	}
	cm.connectionListeners.Store(make([]connectionListener, 0)) //Initialize
	return &cm
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
	if connection.endpoint != nil {
		connectionManager.lock.Lock()
		delete(connectionManager.connections, connection.endpoint.Host()+":"+strconv.Itoa(connection.endpoint.Port()))
		listeners := connectionManager.connectionListeners.Load().([]connectionListener)
		connectionManager.lock.Unlock()
		for _, listener := range listeners {
			if _, ok := listener.(connectionListener); ok {
				listener.(connectionListener).onConnectionClosed(connection, cause)
			}
		}
	} else {
		//Clean up unauthenticated connection
		//TODO::Send the cause as well
		connectionManager.client.InvocationService.cleanupConnection(connection, cause)
	}
}

func (connectionManager *ConnectionManager) NextConnectionId() int64 {
	connectionManager.nextConnectionId = atomic.AddInt64(&connectionManager.nextConnectionId, 1)
	return connectionManager.nextConnectionId
}
func (connectionManager *ConnectionManager) GetOrConnect(address *Address) (chan *Connection, chan error) {
	//TODO:: this is the default address : 127.0.0.1 9701 , add this to config as a default value
	if address == nil {
		address = NewAddress()
	}
	ch := make(chan *Connection, 0)
	err := make(chan error, 1)
	go func() {
		connectionManager.lock.RLock()
		//defer connectionManager.lock.RUnlock()
		if conn, found := connectionManager.connections[address.Host()+":"+strconv.Itoa(address.Port())]; found {
			ch <- conn
			connectionManager.lock.RUnlock()
			return
		}
		connectionManager.lock.RUnlock()
		//Open new connection
		err <- connectionManager.openNewConnection(address, ch)
	}()
	return ch, err
}
func (connectionManager *ConnectionManager) ConnectionCount() int32 {
	connectionManager.lock.RLock()
	defer connectionManager.lock.RUnlock()
	return int32(len(connectionManager.connections))
}
func (connectionManager *ConnectionManager) openNewConnection(address *Address, resp chan *Connection) error {
	connectionManager.lock.Lock()
	defer connectionManager.lock.Unlock()
	invocationService := connectionManager.client.InvocationService
	connectionId := connectionManager.NextConnectionId()
	con := NewConnection(address, invocationService.responseChannel, invocationService.notSentMessages, connectionManager, connectionId)
	if con == nil {
		return common.NewHazelcastTargetDisconnectedError("target is disconnected", nil)
	}
	connectionManager.connections[address.Host()+":"+strconv.Itoa(address.Port())] = con
	err := connectionManager.clusterAuthenticator(con)
	if err != nil {
		return err
	}
	resp <- con
	return nil
}
func (connectionManager *ConnectionManager) clusterAuthenticator(connection *Connection) error {
	uuid := connectionManager.client.ClusterService.uuid
	ownerUuid := connectionManager.client.ClusterService.ownerUuid
	clientType := CLIENT_TYPE
	request := ClientAuthenticationEncodeRequest(
		&connectionManager.client.ClientConfig.GroupConfig.Name,
		&connectionManager.client.ClientConfig.GroupConfig.Password,
		&uuid,
		&ownerUuid,
		true,
		&clientType,
		1,
		//"3.9", //TODO::What should this be ?
	)
	result, err := connectionManager.client.InvocationService.InvokeOnConnection(request, connection).Result()
	if err != nil {
		return err
	} else {

		parameters := ClientAuthenticationDecodeResponse(result)
		if parameters.Status != 0 {
			return common.NewHazelcastAuthenticationError("authentication failed", nil)
		}
		//TODO:: Process the parameters
		connection.serverHazelcastVersion = parameters.ServerHazelcastVersion
		connection.endpoint = parameters.Address
		connection.isOwnerConnection = true
		return nil
	}
	return nil
}
func (connectionManager *ConnectionManager) closeConnection(address core.IAddress, cause error) {
	connectionManager.lock.RLock()
	connection, found := connectionManager.connections[address.Host()+":"+strconv.Itoa(address.Port())]
	connectionManager.lock.RUnlock()
	if found {
		connection.Close(cause)
	}
}

type connectionListener interface {
	onConnectionClosed(connection *Connection, cause error)
	onConnectionOpened(connection *Connection)
}
