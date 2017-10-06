package internal

import (
	"github.com/hazelcast/go-client/core"
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
func (connectionManager *ConnectionManager) GetConnection(address *Address) chan *Connection {
	//TODO:: this is the default address : 127.0.0.1 9701 , add this to config as a default value
	if address == nil {
		address = NewAddress()
	}
	ch := make(chan *Connection, 0)
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
		connectionManager.openNewConnection(address, ch)
	}()
	return ch
}

func (connectionManager *ConnectionManager) openNewConnection(address *Address, resp chan *Connection) {
	connectionManager.lock.Lock()
	defer connectionManager.lock.Unlock()
	invocationService := connectionManager.client.InvocationService
	con := NewConnection(address, invocationService.responseChannel, invocationService.notSentMessages)
	if con == nil {
		close(resp)
		return
	}
	connectionManager.connections[address.Host()+":"+strconv.Itoa(address.Port())] = con
	err := connectionManager.clusterAuthenticator(con)
	if err != nil {
		//TODO ::Handle error
	}
	resp <- con
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
		//TODO:: Handle error
	} else {

		parameters := ClientAuthenticationDecodeResponse(result)
		if parameters.Status != 0 {
			//TODO:: Handle error Authentication failed
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
