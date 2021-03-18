package invocation

import (
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	pubhzerror "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"sync/atomic"
	"time"
)

type Invocation interface {
	Complete(message *proto.ClientMessage)
	Completed() bool
	EventHandler() proto.ClientMessageHandler
	Get() (*proto.ClientMessage, error)
	GetWithTimeout(duration time.Duration) (*proto.ClientMessage, error)
	PartitionID() int32
	Request() *proto.ClientMessage
	Address() pubcluster.Address
}

type Impl struct {
	request         *proto.ClientMessage
	response        chan *proto.ClientMessage
	completed       int32
	boundConnection *Impl
	address         pubcluster.Address
	partitionID     int32
	sentConnection  atomic.Value
	eventHandler    func(clientMessage *proto.ClientMessage)
	deadline        time.Time
}

func NewImpl(clientMessage *proto.ClientMessage, partitionID int32, address pubcluster.Address, timeout time.Duration) *Impl {
	return &Impl{
		partitionID: partitionID,
		address:     address,
		request:     clientMessage,
		response:    make(chan *proto.ClientMessage, 1),
		deadline:    time.Now().Add(timeout),
	}
}

func (i *Impl) Complete(message *proto.ClientMessage) {
	if atomic.CompareAndSwapInt32(&i.completed, 0, 1) {
		i.response <- message
	}
}

func (i *Impl) Completed() bool {
	return i.completed != 0
}

func (i *Impl) EventHandler() proto.ClientMessageHandler {
	return i.eventHandler
}

func (i *Impl) Get() (*proto.ClientMessage, error) {
	response := <-i.response
	return i.unwrapResponse(response)

}

func (i *Impl) GetWithTimeout(duration time.Duration) (*proto.ClientMessage, error) {
	select {
	case response := <-i.response:
		return i.unwrapResponse(response)
	case <-time.After(duration):
		return nil, pubhzerror.NewHazelcastOperationTimeoutError("invocation timed out after "+duration.String(), nil)
	}
}

func (i *Impl) PartitionID() int32 {
	return i.partitionID
}

func (i *Impl) Request() *proto.ClientMessage {
	return i.request
}

func (i *Impl) Address() pubcluster.Address {
	return i.address
}

func (i *Impl) StoreSentConnection(conn interface{}) {
	i.sentConnection.Store(conn)
}

// SetEventHandler sets the event handler for the invocation.
// It should only be called at the site of creation.
func (i *Impl) SetEventHandler(handler proto.ClientMessageHandler) {
	i.eventHandler = handler
}

func (i *Impl) unwrapResponse(response *proto.ClientMessage) (*proto.ClientMessage, error) {
	if response.Err != nil {
		return nil, response.Err
	}
	return response, nil
}
