package cluster

type Listener interface {
	ConnectionOpened(connection Connection)
	ConnectionClosed(connection Connection, err error)
}
