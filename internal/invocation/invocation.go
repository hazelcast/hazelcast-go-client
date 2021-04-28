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

	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"

	"github.com/hazelcast/hazelcast-go-client/internal/cb"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
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
	PartitionID() int32
	Request() *proto.ClientMessage
	Address() *pubcluster.AddressImpl
	Close()
	CanRetry(err error) bool
}

type Impl struct {
	request       *proto.ClientMessage
	response      chan *proto.ClientMessage
	completed     int32
	address       *pubcluster.AddressImpl
	partitionID   int32
	eventHandler  func(clientMessage *proto.ClientMessage)
	deadline      time.Time
	redoOperation bool
}

func NewImpl(clientMessage *proto.ClientMessage, partitionID int32, address *pubcluster.AddressImpl, deadline time.Time, redoOperation bool) *Impl {
	return &Impl{
		partitionID:   partitionID,
		address:       address,
		request:       clientMessage,
		response:      make(chan *proto.ClientMessage, 1),
		deadline:      deadline,
		redoOperation: redoOperation,
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
	ctx, cancel := context.WithDeadline(context.Background(), i.deadline)
	msg, err := i.GetWithContext(ctx)
	cancel()
	return msg, err
}

func (i *Impl) GetWithContext(ctx context.Context) (*proto.ClientMessage, error) {
	select {
	case response, ok := <-i.response:
		if ok {
			return i.unwrapResponse(response)
		}
		return nil, cb.WrapNonRetryableError(ErrResponseChannelClosed)
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil && !i.CanRetry(err) {
			err = cb.WrapNonRetryableError(err)
		}
		return nil, err
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

func (i *Impl) CanRetry(err error) bool {
	var nonRetryableError *cb.NonRetryableError
	if errors.Is(err, nonRetryableError) {
		return false
	}

	// TODO: Check the following condition when invoke on member is implemented
	/*
		var targetNotMemberError *hzerror.HazelcastTargetNotMemberError
		if (uuid != null && t instanceof TargetNotMemberException) {
			//when invocation send to a specific member
			//if target is no longer a member, we should not retry
			//note that this exception could come from the server
			return false;
		}
	*/

	var ioError *hzerror.HazelcastIOError
	var instanceNotActiveError *hzerror.HazelcastInstanceNotActiveError
	if errors.Is(err, ioError) || errors.Is(err, instanceNotActiveError) {
		return true
	}

	var targetDisconnectedError *hzerror.HazelcastTargetDisconnectedError
	if errors.Is(err, targetDisconnectedError) {
		return i.Request().Retryable || i.redoOperation
	}
	return false
}

func (i *Impl) unwrapResponse(response *proto.ClientMessage) (*proto.ClientMessage, error) {
	if response.Err != nil {
		if i.CanRetry(response.Err) {
			return nil, response.Err
		}
		return nil, cb.WrapNonRetryableError(response.Err)
	}
	return response, nil
}
