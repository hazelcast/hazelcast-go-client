package invocation

import (
	"context"
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type Invocation interface {
	Complete(message *proto.ClientMessage)
	Completed() bool
	EventHandler() proto.ClientMessageHandler
	Get() (*proto.ClientMessage, error)
	GetContext(ctx context.Context) (*proto.ClientMessage, error)
	GetWithTimeout(duration time.Duration) (*proto.ClientMessage, error)
	PartitionID() int32
	Request() *proto.ClientMessage
	Address() *pubcluster.AddressImpl
}

type Impl struct {
	request         *proto.ClientMessage
	response        chan *proto.ClientMessage
	completed       int32
	boundConnection *Impl
	address         *pubcluster.AddressImpl
	partitionID     int32
	//sentConnection  atomic.Value
	eventHandler func(clientMessage *proto.ClientMessage)
	deadline     time.Time
}

func NewImpl(clientMessage *proto.ClientMessage, partitionID int32, address *pubcluster.AddressImpl, timeout time.Duration) *Impl {
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

func (i *Impl) GetContext(ctx context.Context) (*proto.ClientMessage, error) {
	select {
	case response, ok := <-i.response:
		if ok {
			return i.unwrapResponse(response)
		}
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (i *Impl) GetWithTimeout(duration time.Duration) (*proto.ClientMessage, error) {
	select {
	case response := <-i.response:
		return i.unwrapResponse(response)
	case <-time.After(duration):
		return nil, hzerror.NewHazelcastOperationTimeoutError("invocation timed out after "+duration.String(), nil)
	}
}

func (i *Impl) PartitionID() int32 {
	return i.partitionID
}

func (i *Impl) Request() *proto.ClientMessage {
	return i.request
}

func (i *Impl) Address() *pubcluster.AddressImpl {
	return i.address
}

/*
func (i *Proxy) StoreSentConnection(conn interface{}) {
	i.sentConnection.Store(conn)
}
*/

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
