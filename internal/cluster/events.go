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

package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/cluster"
	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
)

const (
	// EventConnectionOpened is dispatched when a connection to a member is opened.
	EventConnectionOpened = "internal.cluster.connectionopened"
	// EventConnectionClosed is dispatched when a connection to a member is closed.
	EventConnectionClosed = "internal.cluster.connectionclosed"

	// EventMembersUpdated is dispatched when cluster service receives MembersUpdated event from the server
	EventMembersUpdated = "internal.cluster.membersupdated"
	// EventPartitionsUpdated when cluster service receives PartitionsUpdated event from the server
	EventPartitionsUpdated = "internal.cluster.partitionsupdates"

	// EventMembersAdded is dispatched when cluster service finds out new members are added to the cluster
	EventMembersAdded = "internal.cluster.membersadded"
	// EventMembersAdded is dispatched when cluster service finds out new members are removed from the cluster
	EventMembersRemoved = "internal.cluster.membersremoved"

	// EventConnected is dispatched after the very first connection to the cluster or the first connection after client disconnected.
	EventConnected = "internal.cluster.connected"

	// EventDisconnected is dispatched when all connections to the cluster are closed.
	EventDisconnected = "internal.cluster.disconnected"
)

type ConnectionOpenedHandler func(event *ConnectionOpened)
type ConnectionClosedHandler func(event *ConnectionClosed)
type ConnectedHandler func(event *Connected)
type DisconnectedHandler func(event *Disconnected)

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

type Connected struct {
}

func NewConnected() *Connected {
	return &Connected{}
}

func (e *Connected) EventName() string {
	return EventConnected
}

type Disconnected struct {
}

func NewDisconnected() *Disconnected {
	return &Disconnected{}
}

func (c *Disconnected) EventName() string {
	return EventDisconnected
}
