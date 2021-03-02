package connection

type Listener interface {
	ConnectionOpened(connection *Impl)
	ConnectionClosed(connection *Impl, err error)
}
