package proto

type ClientMessageHandler func(clientMessage *ClientMessage)

type ListenerBinder interface {
	Add(request *ClientMessage, clientRegistrationID int, handler ClientMessageHandler) error
	Remove(mapName string, clientRegistrationID int) error
}
