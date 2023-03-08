//go:build hazelcastinternal
// +build hazelcastinternal

/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package hazelcast

import (
	"context"
	"fmt"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/client"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

/*
WARNING!
The constants, types, methods and functions defined under hazelcastinternal are considered internal API.
No backward-compatibility guarantees apply for this code.
*/

type Data = serialization.Data
type ClientMessage = proto.ClientMessage
type ClientMessageHandler = proto.ClientMessageHandler
type Frame = proto.Frame
type ForwardFrameIterator = proto.ForwardFrameIterator
type Pair = proto.Pair

var (
	NullFrame  = NewFrameWith([]byte{}, IsNullFlag)
	BeginFrame = NewFrameWith([]byte{}, BeginDataStructureFlag)
	EndFrame   = NewFrameWith([]byte{}, EndDataStructureFlag)
)

func NewClientMessageForEncode() *ClientMessage {
	return proto.NewClientMessageForEncode()
}

func NewFrame(content []byte) Frame {
	return proto.NewFrame(content)
}

func NewFrameWith(content []byte, flags uint16) Frame {
	return proto.NewFrameWith(content, flags)
}

func NewPair(key, value interface{}) Pair {
	return Pair{Key: key, Value: value}
}

type InvokeOptions struct {
	// Handler is the event handler for an invocation.
	Handler ClientMessageHandler
}

// ClientInternal is an accessor for a Client.
type ClientInternal struct {
	client  *Client
	invoker *client.Invoker
}

// NewClientInternal creates the client internal accessor with the given client.
func NewClientInternal(c *Client) *ClientInternal {
	return &ClientInternal{
		client:  c,
		invoker: c.proxyManager.invoker,
	}
}

// Client returns the wrapped Client instance.
func (ci *ClientInternal) Client() *Client {
	return ci.client
}

// ClusterID returns the cluster ID.
// It returns zero value of types.UUID{} if the cluster ID does not exist.
func (ci *ClientInternal) ClusterID() types.UUID {
	return ci.client.ic.ConnectionManager.ClusterID()
}

// OrderedMembers returns the most recent member list of the cluster.
func (ci *ClientInternal) OrderedMembers() []pubcluster.MemberInfo {
	return ci.client.ic.ClusterService.OrderedMembers()
}

// ConnectedToMember returns true if there is a connection to the given member.
func (ci *ClientInternal) ConnectedToMember(uuid types.UUID) bool {
	return ci.client.ic.ConnectionManager.GetConnectionForUUID(uuid) != nil
}

// EncodeData serializes the given value and returns a Data value.
func (ci *ClientInternal) EncodeData(obj interface{}) (Data, error) {
	return ci.SerializationService().ToData(obj)
}

// DecodeData deserializes the given Data and returns a value.
func (ci *ClientInternal) DecodeData(data Data) (interface{}, error) {
	return ci.SerializationService().ToObject(data)
}

// InvokeOnRandomTarget sends the given request to one of the members.
// If opts.Handler is given, it is used as an event listener handler.
func (ci *ClientInternal) InvokeOnRandomTarget(ctx context.Context, request *ClientMessage, opts *InvokeOptions) (*ClientMessage, error) {
	var handler proto.ClientMessageHandler
	if opts != nil {
		handler = opts.Handler
	}
	return ci.invoker.InvokeOnRandomTarget(ctx, request, handler)
}

// InvokeOnPartition sends the given request to the member which has the given partition ID.
func (ci *ClientInternal) InvokeOnPartition(ctx context.Context, request *ClientMessage, partitionID int32, opts *InvokeOptions) (*ClientMessage, error) {
	return ci.invoker.InvokeOnPartition(ctx, request, partitionID)
}

// InvokeOnKey sends the given request to the member which corresponds to the given key.
func (ci *ClientInternal) InvokeOnKey(ctx context.Context, request *proto.ClientMessage, keyData Data, opts *InvokeOptions) (*proto.ClientMessage, error) {
	partitionID, err := ci.GetPartitionID(keyData)
	if err != nil {
		return nil, err
	}
	return ci.invoker.InvokeOnPartition(ctx, request, partitionID)
}

// InvokeOnMember sends the request to the given member.
// If opts.Handler is given, it is used as an event listener handler.
func (ci *ClientInternal) InvokeOnMember(ctx context.Context, request *ClientMessage, uuid types.UUID, opts *InvokeOptions) (*ClientMessage, error) {
	var handler proto.ClientMessageHandler
	if opts != nil {
		handler = opts.Handler
	}
	mem := ci.client.ic.ClusterService.GetMemberByUUID(uuid)
	if mem == nil {
		return nil, hzerrors.NewIllegalArgumentError(fmt.Sprintf("member not found: %s", uuid.String()), nil)
	}
	now := time.Now()
	return ci.invoker.TryInvoke(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if attempt > 0 {
			request = request.Copy()
		}
		inv := ci.invoker.Factory().NewMemberBoundInvocation(request, mem, now)
		inv.SetEventHandler(handler)
		if err := ci.invoker.SendInvocation(ctx, inv); err != nil {
			return nil, err
		}
		return inv.GetWithContext(ctx)
	})
}

// GetPartitionID returns the partition ID for the given Data value.
// The returned error may be ignored.
func (ci *ClientInternal) GetPartitionID(data Data) (int32, error) {
	return ci.client.ic.PartitionService.GetPartitionID(data)
}

// PartitionCount returns the partition count.
func (ci *ClientInternal) PartitionCount() int32 {
	return ci.client.ic.PartitionService.PartitionCount()
}

const (
	TypeFieldOffset                      = proto.TypeFieldOffset
	MessageTypeOffset                    = proto.MessageTypeOffset
	ByteSizeInBytes                      = proto.ByteSizeInBytes
	BooleanSizeInBytes                   = proto.BooleanSizeInBytes
	ShortSizeInBytes                     = proto.ShortSizeInBytes
	CharSizeInBytes                      = proto.CharSizeInBytes
	IntSizeInBytes                       = proto.IntSizeInBytes
	FloatSizeInBytes                     = proto.FloatSizeInBytes
	LongSizeInBytes                      = proto.LongSizeInBytes
	DoubleSizeInBytes                    = proto.DoubleSizeInBytes
	UUIDSizeInBytes                      = proto.UUIDSizeInBytes
	UuidSizeInBytes                      = proto.UuidSizeInBytes // Deprecated
	EntryListUUIDLongEntrySizeInBytes    = proto.EntryListUUIDLongEntrySizeInBytes
	EntryListIntegerLongSizeInBytes      = proto.EntryListIntegerLongSizeInBytes
	EntryListIntegerUUIDEntrySizeInBytes = proto.EntryListIntegerUUIDEntrySizeInBytes
	LocalDateSizeInBytes                 = proto.LocalDateSizeInBytes
	LocalTimeSizeInBytes                 = proto.LocalTimeSizeInBytes
	LocalDateTimeSizeInBytes             = proto.LocalDateTimeSizeInBytes
	OffsetDateTimeSizeInBytes            = proto.OffsetDateTimeSizeInBytes
	CorrelationIDFieldOffset             = proto.CorrelationIDFieldOffset
	CorrelationIDOffset                  = proto.CorrelationIDOffset
	FragmentationIDOffset                = proto.FragmentationIDOffset
	PartitionIDOffset                    = proto.PartitionIDOffset
	RequestThreadIdOffset                = proto.RequestThreadIDOffset
	RequestTtlOffset                     = proto.RequestTTLOffset
	RequestIncludeValueOffset            = proto.RequestIncludeValueOffset
	RequestListenerFlagsOffset           = proto.RequestListenerFlagsOffset
	RequestLocalOnlyOffset               = proto.RequestLocalOnlyOffset
	RequestReferenceIdOffset             = proto.RequestReferenceIdOffset
	ResponseBackupAcksOffset             = proto.ResponseBackupAcksOffset
	UnfragmentedMessage                  = proto.UnfragmentedMessage
	DefaultFlags                         = proto.DefaultFlags
	BeginFragmentFlag                    = proto.BeginFragmentFlag
	EndFragmentFlag                      = proto.EndFragmentFlag
	IsFinalFlag                          = proto.IsFinalFlag
	BeginDataStructureFlag               = proto.BeginDataStructureFlag
	EndDataStructureFlag                 = proto.EndDataStructureFlag
	IsNullFlag                           = proto.IsNullFlag
	IsEventFlag                          = proto.IsEventFlag
	BackupEventFlag                      = proto.BackupEventFlag
	SizeOfFrameLengthAndFlags            = proto.SizeOfFrameLengthAndFlags
)
