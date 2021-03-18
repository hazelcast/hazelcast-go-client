package proto

type ClientMessageHandler func(clientMessage *ClientMessage)

type ListenerBinder interface {
	Add(request *ClientMessage, handler ClientMessageHandler) error
}
