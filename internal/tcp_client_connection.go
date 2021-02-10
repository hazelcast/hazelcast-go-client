package internal

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/util/timeutil"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type TcpClientConnection interface {
	Start()
	Write(message *proto.ClientMessage) bool
}

type tcpClientConnection struct {
	Connection
	client          *HazelcastClient
	connectionId    int64
	conn            net.Conn
	pending         chan *proto.ClientMessage
	received        chan *proto.ClientMessage
	closed          chan struct{}
	startTime       int64
	lastRead        atomic.Value
	lastWrite       atomic.Value
	closedTime      atomic.Value
	status          int32
	eventHandlers   map[int64]ClientInvocation
	eventHandlersMU sync.RWMutex
}

func NewTcpClientConnection(client *HazelcastClient, connectionId int64, conn net.Conn) TcpClientConnection {
	t := &tcpClientConnection{
		client:       client,
		connectionId: connectionId,
		conn:         conn,
		pending:      make(chan *proto.ClientMessage, 1),
		received:     make(chan *proto.ClientMessage, 1),
		closed:       make(chan struct{}),
		status:       0,
	}
	t.lastWrite.Store(time.Time{})
	t.closedTime.Store(time.Time{})
	t.startTime = timeutil.GetCurrentTimeInMilliSeconds()
	t.lastRead.Store(time.Now())
	t.sendProtocolStarter()

	go t.handleClientMessage()

	return t
}

func (c *tcpClientConnection) Start() {

}

func (c *tcpClientConnection) Write(message *proto.ClientMessage) bool {
	select {
	case <-c.closed:
		return false
	case c.pending <- message:
		return true
	}
}

func (c *tcpClientConnection) sendProtocolStarter() {
	c.conn.Write([]byte(protocolStarter))
}

func (c *tcpClientConnection) isAlive() bool {
	return atomic.LoadInt32(&c.status) == 0
}

func (c *tcpClientConnection) handleClientMessage() {

}
