package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/cluster"
	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	// EventConnectionOpened is dispatched when a connection to a member is opened.
	EventConnectionOpened = "internal.cluster.connectionopened"
	// EventConnectionClosed is dispatched when a connection to a member is closed.
	EventConnectionClosed = "internal.cluster.connectionclosed"
	// EventOwnerConnectionChanged is dispatched when the owner connection is opened.
	EventOwnerConnectionChanged = "internal.cluster.ownerconnectionchanged"
	// EventOwnerConnectionClosed is dispatched when the owner connection is closed.
	// This event is dispatch in addition to EventConnectionClosed.
	EventOwnerConnectionClosed = "internal.cluster.ownerconnectionclosed"

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

type ConnectionOpenedHandler func(event *ConnectionOpened)
type ConnectionClosedHandler func(event *ConnectionClosed)
type OwnerConnectionChangedHandler func(event *OwnerConnectionChanged)
type OwnerConnectionClosedHandler func(event *OwnerConnectionClosed)

type ConnectionOpened struct {
	Conn *Connection
}

func NewConnectionOpened(conn *Connection) *ConnectionOpened {
	return &ConnectionOpened{Conn: conn}
}

func (c ConnectionOpened) EventName() string {
	return EventConnectionOpened
}

type ConnectionClosed struct {
	Conn *Connection
	Err  error
}

func NewConnectionClosed(conn *Connection, err error) *ConnectionClosed {
	return &ConnectionClosed{
		Conn: conn,
		Err:  err,
	}
}

func (c ConnectionClosed) EventName() string {
	return EventConnectionClosed
}

type OwnerConnectionChanged struct {
	Conn *Connection
}

func NewOwnerConnectionChanged(conn *Connection) *OwnerConnectionChanged {
	return &OwnerConnectionChanged{Conn: conn}
}

func (c OwnerConnectionChanged) EventName() string {
	return EventOwnerConnectionChanged
}

type OwnerConnectionClosed struct {
	Conn *Connection
	Err  error
}

func NewOwnerConnectionClosed(conn *Connection, err error) *ConnectionClosed {
	return &ConnectionClosed{
		Conn: conn,
		Err:  err,
	}
}

func (c OwnerConnectionClosed) EventName() string {
	return EventOwnerConnectionClosed
}

type MembersAdded struct {
	Members []pubcluster.Member
}

func NewMembersAdded(members []pubcluster.Member) *MembersAdded {
	return &MembersAdded{Members: members}
}

func (m MembersAdded) EventName() string {
	return EventMembersAdded
}

type MembersRemoved struct {
	Members []pubcluster.Member
}

func NewMemberRemoved(members []cluster.Member) *MembersRemoved {
	return &MembersRemoved{Members: members}
}

func (m MembersRemoved) EventName() string {
	return EventMembersRemoved
}

type PartitionsUpdated struct {
	Partitions []proto.Pair
	Version    int32
}

func NewPartitionsUpdated(pairs []proto.Pair, version int32) *PartitionsUpdated {
	return &PartitionsUpdated{
		Partitions: pairs,
		Version:    version,
	}
}

func (p PartitionsUpdated) EventName() string {
	return EventPartitionsUpdated
}

type MembersUpdated struct {
	Members []cluster.MemberInfo
	Version int32
}

func NewMembersUpdated(memberInfos []cluster.MemberInfo, version int32) *MembersUpdated {
	return &MembersUpdated{
		Members: memberInfos,
		Version: version,
	}
}

func (m MembersUpdated) EventName() string {
	return EventMembersUpdated
}

type PartitionsLoaded struct {
}

func NewPartitionsLoaded() *PartitionsLoaded {
	return &PartitionsLoaded{}
}

func (p PartitionsLoaded) EventName() string {
	return EventPartitionsLoaded
}
