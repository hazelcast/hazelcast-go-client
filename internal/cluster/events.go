package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
)

const (
	EventConnectionOpened = "internal.cluster.connectionopened"
	EventConnectionClosed = "internal.cluster.connectionclosed"
)

type ConnectionOpened interface {
	event.Event
	Conn() Connection
}

type ConnectionClosed interface {
	event.Event
	Conn() Connection
	Err() error
}

type ConnectionOpenedHandler func(event ConnectionOpened)
type ConnectionClosedHandler func(event ConnectionClosed)

type ConnectionOpenedImpl struct {
	conn *ConnectionImpl
}

func NewConnectionOpened(conn *ConnectionImpl) *ConnectionOpenedImpl {
	return &ConnectionOpenedImpl{conn: conn}
}

func (c ConnectionOpenedImpl) Name() string {
	return EventConnectionOpened
}

func (c ConnectionOpenedImpl) Conn() Connection {
	return c.conn
}

type ConnectionClosedImpl struct {
	conn *ConnectionImpl
	err  error
}

func NewConnectionClosed(conn *ConnectionImpl, err error) *ConnectionClosedImpl {
	return &ConnectionClosedImpl{
		conn: conn,
		err:  err,
	}
}

func (c ConnectionClosedImpl) Name() string {
	return EventConnectionClosed
}

func (c ConnectionClosedImpl) Conn() Connection {
	return c.conn
}

func (c ConnectionClosedImpl) Err() error {
	return c.err
}
