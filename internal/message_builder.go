package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/common"
)
type ClientMessageBuilder struct {
	incompleteMessages map[int64]*ClientMessage
	responseChannel chan *ClientMessage
}

func (cmb *ClientMessageBuilder) OnMessage(msg *ClientMessage) {
	if msg.HasFlags(common.BEGIN_END_FLAG) >0{
		cmb.responseChannel <- msg
	}else if msg.HasFlags(common.BEGIN_FLAG)>0{
		cmb.incompleteMessages[msg.CorrelationId()]= msg
	}else {
		message,found :=cmb.incompleteMessages[msg.CorrelationId()]
		if !found {
			//TODO:: Handle case : a message without a begin part is received.
		}
		message.Accumulate(msg)
		if msg.HasFlags(common.END_FLAG)>0{
			message.AddFlags(common.BEGIN_END_FLAG)
			cmb.responseChannel <- message
			delete(cmb.incompleteMessages,msg.CorrelationId())
		}
	}
}