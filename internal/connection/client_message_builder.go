package connection

import "github.com/hazelcast/hazelcast-go-client/v4/internal/proto"

type clientMessageBuilder struct {
	incompleteMessages map[int64]*proto.ClientMessage
	handleResponse     func(interface{})
}

func (mb *clientMessageBuilder) onMessage(msg *proto.ClientMessage) {

	if msg.StartFrame.HasUnFragmentedMessageFlags() {
		mb.handleResponse(msg)
	} else {
		println("here")
	}

	//TODO
	/**
		if msg.HasFlags(bufutil.BeginEndFlag) > 0 {
		mb.handleResponse(msg)
	} else if msg.HasFlags(bufutil.BeginFlag) > 0 {
		mb.incompleteMessages[msg.CorrelationID()] = msg
	} else {
		message, found := mb.incompleteMessages[msg.CorrelationID()]
		if !found {
			return
		}
		message.Accumulate(msg)
		if msg.HasFlags(bufutil.EndFlag) > 0 {
			message.AddFlags(bufutil.BeginEndFlag)
			mb.handleResponse(message)
			delete(mb.incompleteMessages, msg.CorrelationID())
		}
	}
	*/

}
