package internal

import "github.com/hazelcast/go-client"

type ClientMessageBuilder struct {
	incompleteMessages map[int64]*hazelcast.ClientMessage
	MessageCallback func(hazelcast.ClientMessage)
}

func (cmb *ClientMessageBuilder) OnMessage(msg *hazelcast.ClientMessage) {
//TODO implementation
}