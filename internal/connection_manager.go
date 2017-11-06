package internal

import (
	"github.com/hazelcast/go-client/core"
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"strconv"
	"sync"
)

type ConnectionManager struct {
	client       *HazelcastClient
	connections  map[string]*Connection
	ownerAddress *Address
	lock         sync.RWMutex
}

func NewConnectionManager(client *HazelcastClient) *ConnectionManager {
	cm := ConnectionManager{client: client,
		connections: make(map[string]*Connection),
	}
	return &cm
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
	con := NewConnection(address, invocationService.responseChannel, invocationService.notSentMessages)
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
		connection.endpoint = parameters.Address
		connection.isOwnerConnection = true
		return nil
	}
	return nil
}
func (connectionManager *ConnectionManager) closeConnection(address core.IAddress) {
	connectionManager.lock.RLock()
	defer connectionManager.lock.RUnlock()
	connection, found := connectionManager.connections[address.Host()+":"+strconv.Itoa(address.Port())]
	if found {
		connection.Close()
	}
}
