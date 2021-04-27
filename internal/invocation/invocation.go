/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package invocation

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

var ErrResponseChannelClosed = errors.New("response channel closed")

type Result interface {
	Get() (*proto.ClientMessage, error)
	GetWithTimeout(duration time.Duration) (*proto.ClientMessage, error)
}

type Invocation interface {
	Complete(message *proto.ClientMessage)
	Completed() bool
	EventHandler() proto.ClientMessageHandler
	Get() (*proto.ClientMessage, error)
	GetWithContext(ctx context.Context) (*proto.ClientMessage, error)
	GetWithTimeout(duration time.Duration) (*proto.ClientMessage, error)
	PartitionID() int32
	Request() *proto.ClientMessage
	Address() *pubcluster.AddressImpl
	Close()
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
	if response, ok := <-i.response; ok {
		return i.unwrapResponse(response)
	}
	return nil, ErrResponseChannelClosed
}

func (i *Impl) GetWithContext(ctx context.Context) (*proto.ClientMessage, error) {
	select {
	case response, ok := <-i.response:
		if ok {
			return i.unwrapResponse(response)
		}
		return nil, ErrResponseChannelClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (i *Impl) GetWithTimeout(duration time.Duration) (*proto.ClientMessage, error) {
	select {
	case response, ok := <-i.response:
		if ok {
			return i.unwrapResponse(response)
		}
		return nil, ErrResponseChannelClosed
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

func (i *Impl) Close() {
	close(i.response)
}

func (i *Impl) unwrapResponse(response *proto.ClientMessage) (*proto.ClientMessage, error) {
	if response.Err != nil {
		return nil, response.Err
	}
	return response, nil
}
