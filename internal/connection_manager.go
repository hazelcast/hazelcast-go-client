package internal

import (
	"sync"

	. "github.com/hazelcast/go-client/internal/protocol"
)

type ConnectionManager struct {
	client       *HazelcastClient
	connections  map[*Address]*Connection
	ownerAddress *Address
	lock         sync.RWMutex
}

func (connectionManager *ConnectionManager) GetConnection(address *Address) chan *Connection {
	ch := make(chan *Connection, 1)
	go func() {
		connectionManager.lock.RLock()
		//defer connectionManager.lock.RUnlock()

		if conn, found := connectionManager.connections[address]; found {
			ch <- conn
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
	resp <- NewConnection(address, invocationService.responseChannel, invocationService.notSentMessages)
}
