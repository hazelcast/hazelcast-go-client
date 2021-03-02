package invocation

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"sync/atomic"
	"time"
)

type EventHandler func(clientMessage *proto.ClientMessage)

type Invocation interface {
	Complete(message *proto.ClientMessage)
	CompleteWithErr(err error)
	Completed() bool
	EventHandler() EventHandler
	Get() (*proto.ClientMessage, error)
	GetWithTimeout(duration time.Duration) (*proto.ClientMessage, error)
	PartitionID() int32
	Request() *proto.ClientMessage
	Address() *core.Address
	StoreSentConnection(conn interface{})
}

type Impl struct {
	request         atomic.Value
	response        chan interface{}
	completed       int32
	boundConnection *Impl
	address         *core.Address
	partitionID     int32
	sentConnection  atomic.Value
	eventHandler    func(clientMessage *proto.ClientMessage)
	deadline        time.Time
}

func NewImpl(clientMessage *proto.ClientMessage, partitionID int32, address *core.Address,
	timeout time.Duration) *Impl {
	inv := &Impl{
		partitionID: partitionID,
		address:     address,
		response:    make(chan interface{}, 1),
		deadline:    time.Now().Add(timeout),
	}
	inv.request.Store(clientMessage)
	return inv
}

func (i *Impl) Complete(message *proto.ClientMessage) {
	panic("implement me")
}

func (i *Impl) CompleteWithErr(err error) {
	panic("implement me")
}

func (i *Impl) Completed() bool {
	return i.completed != 0
}

func (i *Impl) EventHandler() EventHandler {
	return i.eventHandler
}

func (i *Impl) Get() (*proto.ClientMessage, error) {
	response := <-i.response
	return i.unwrapResponse(response)

}

func (i *Impl) GetWithTimeout(duration time.Duration) (*proto.ClientMessage, error) {
	for {
		select {
		case response := <-i.response:
			return i.unwrapResponse(response)
		case <-time.After(duration):
			return nil, core.NewHazelcastOperationTimeoutError("invocation timed out after "+duration.String(), nil)
		}
	}
}

func (i Impl) PartitionID() int32 {
	return i.partitionID
}

func (i Impl) Request() *proto.ClientMessage {
	return i.request.Load().(*proto.ClientMessage)
}

func (i Impl) Address() *core.Address {
	return i.address
}

func (i Impl) StoreSentConnection(conn interface{}) {
	i.sentConnection.Store(conn)
}

func (i *Impl) unwrapResponse(response interface{}) (*proto.ClientMessage, error) {
	switch res := response.(type) {
	case *proto.ClientMessage:
		return res, nil
	case error:
		return nil, res
	default:
		panic("Unexpected response in invocation ")
	}
}
