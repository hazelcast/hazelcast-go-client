package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
)
type ClientMessageBuilder struct {
	incompleteMessages map[int64]*ClientMessage
	MessageCallback func(ClientMessage)
}

func (cmb *ClientMessageBuilder) OnMessage(msg *ClientMessage) {
//TODO implementation
}