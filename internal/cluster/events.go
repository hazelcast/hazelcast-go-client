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
	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
)

const (
	// EventConnection is dispatched when a connection to a member is opened/closed.
	EventConnection = "internal.cluster.connection"

	// EventMembers is dispatched when cluster service finds out new members are added to the cluster
	// or when members are removed from cluster
	EventMembers = "internal.cluster.members"

	// EventCluster is dispatched after the very first connection to the cluster or the first connection after client disconnected.
	//and  dispatched when all connections to the cluster are closed.
	EventCluster = "internal.cluster.cluster"
)

type ConnectionEventHandler func(event *ConnectionEvent)
type ConnectedHandler func(event *ClusterEvent)

type ConnectionEvent struct {
	Conn   *Connection
	Err    error
	Opened bool
}

func NewConnectionOpened(conn *Connection) *ConnectionEvent {
	return &ConnectionEvent{Conn: conn, Err: nil, Opened: true}
}

func NewConnectionClosed(conn *Connection, err error) *ConnectionEvent {
	return &ConnectionEvent{
		Conn:   conn,
		Err:    err,
		Opened: false,
	}
}

func (c ConnectionEvent) EventName() string {
	return EventConnection
}

type MembersEvent struct {
	Members []pubcluster.MemberInfo
	Added   bool
}

func NewMembersAdded(members []pubcluster.MemberInfo) *MembersEvent {
	return &MembersEvent{Members: members, Added: true}
}

func (m MembersEvent) EventName() string {
	return EventMembers
}

func NewMemberRemoved(members []pubcluster.MemberInfo) *MembersEvent {
	return &MembersEvent{Members: members, Added: false}
}

type ClusterEvent struct {
	Addr* pubcluster.Address
	Connected bool
}

func NewConnected(addr* pubcluster.Address) *ClusterEvent {
	return &ClusterEvent{Addr: addr, Connected: true}
}

func (e *ClusterEvent) EventName() string {
	return EventCluster
}

func NewDisconnected() *ClusterEvent {
	return &ClusterEvent{Addr: nil, Connected: false}
}
