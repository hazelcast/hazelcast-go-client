package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// EventConnectionOpened is dispatched when a connection to a member is opened
	EventConnectionOpened = "internal.cluster.connectionopened"
	// EventConnectionClosed is dispatched when a connection to a member is closed
	EventConnectionClosed = "internal.cluster.connectionclosed"

	// EventMembersUpdated is dispatched when cluster service receives MembersUpdated event from the server
	EventMembersUpdated = "internal.cluster.membersupdated"
	// EventPartitionsUpdated when cluster service receives PartitionsUpdated event from the server
	EventPartitionsUpdated = "internal.cluster.partitionsupdates"

	// EventMembersAdded is dispatched when cluster service finds out new members are added to the cluster
	EventMembersAdded = "internal.cluster.membersadded"
	// EventMembersAdded is dispatched when cluster service finds out new members are removed from the cluster
	EventMembersRemoved = "internal.cluster.membersremoved"

	// EventPartitionsLoaded is dispatched when partition service updates its partition table
	// This is required to enable smart routing
	EventPartitionsLoaded = "internal.cluster.partitionsloaded"
)

type ConnectionOpened interface {
	event.Event
	Conn() *Connection
}

type ConnectionClosed interface {
	event.Event
	Conn() *Connection
	Err() error
}

type MembersUpdated interface {
	event.Event
	Members() []cluster.MemberInfo
	Version() int32
}

type MembersAdded interface {
	event.Event
	Members() []cluster.Member
}

type MembersRemoved interface {
	event.Event
	Members() []cluster.Member
}

type PartitionsUpdated interface {
	event.Event
	Partitions() []proto.Pair
	Version() int32
}

type ConnectionOpenedHandler func(event ConnectionOpened)
type ConnectionClosedHandler func(event ConnectionClosed)

type ConnectionOpenedImpl struct {
	conn *Connection
}

func NewConnectionOpened(conn *Connection) *ConnectionOpenedImpl {
	return &ConnectionOpenedImpl{conn: conn}
}

func (c ConnectionOpenedImpl) Name() string {
	return EventConnectionOpened
}

func (c ConnectionOpenedImpl) Conn() *Connection {
	return c.conn
}

type ConnectionClosedImpl struct {
	conn *Connection
	err  error
}

func NewConnectionClosed(conn *Connection, err error) *ConnectionClosedImpl {
	return &ConnectionClosedImpl{
		conn: conn,
		err:  err,
	}
}

func (c ConnectionClosedImpl) Name() string {
	return EventConnectionClosed
}

func (c ConnectionClosedImpl) Conn() *Connection {
	return c.conn
}

func (c ConnectionClosedImpl) Err() error {
	return c.err
}

type MemberAddedImpl struct {
	members []cluster.Member
}

func NewMembersAdded(members []cluster.Member) *MemberAddedImpl {
	return &MemberAddedImpl{members: members}
}

func (m MemberAddedImpl) Name() string {
	return EventMembersAdded
}

func (m MemberAddedImpl) Members() []cluster.Member {
	return m.members
}

type MemberRemovedImpl struct {
	members []cluster.Member
}

func (m MemberRemovedImpl) Name() string {
	return EventMembersRemoved
}

func (m MemberRemovedImpl) Members() []cluster.Member {
	return m.members
}

func NewMemberRemoved(members []cluster.Member) *MemberRemovedImpl {
	return &MemberRemovedImpl{members: members}
}

type PartitionsUpdatedImpl struct {
	pairs   []proto.Pair
	version int32
}

func NewPartitionsUpdated(pairs []proto.Pair, version int32) *PartitionsUpdatedImpl {
	return &PartitionsUpdatedImpl{
		pairs:   pairs,
		version: version,
	}
}

func (p PartitionsUpdatedImpl) Name() string {
	return EventPartitionsUpdated
}

func (p PartitionsUpdatedImpl) Partitions() []proto.Pair {
	return p.pairs
}

func (p PartitionsUpdatedImpl) Version() int32 {
	return p.version
}

type MembersUpdatedImpl struct {
	memberInfos []cluster.MemberInfo
	version     int32
}

func NewMembersUpdated(memberInfos []cluster.MemberInfo, version int32) *MembersUpdatedImpl {
	return &MembersUpdatedImpl{
		memberInfos: memberInfos,
		version:     version,
	}
}

func (m MembersUpdatedImpl) Name() string {
	return EventMembersUpdated
}

func (m MembersUpdatedImpl) Members() []cluster.MemberInfo {
	return m.memberInfos
}

func (m MembersUpdatedImpl) Version() int32 {
	return m.version
}

type PartitionsLoadedImpl struct {
}

func NewPartitionsLoaded() *PartitionsLoadedImpl {
	return &PartitionsLoadedImpl{}
}

func (p PartitionsLoadedImpl) Name() string {
	return EventPartitionsLoaded
}
