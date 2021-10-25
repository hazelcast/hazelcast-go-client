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

type ConnectionEventHandler func(event *ConnectionStateChangedEvent)
type ConnectedHandler func(event *ClusterStateChangedEvent)

type ConnectionState int

const (
	ConnectionStateOpened ConnectionState = iota
	ConnectionStateClosed
)

type ConnectionStateChangedEvent struct {
	Conn  *Connection
	Err   error
	state ConnectionState
}

func NewConnectionOpened(conn *Connection) *ConnectionStateChangedEvent {
	return &ConnectionStateChangedEvent{Conn: conn, Err: nil, state: ConnectionStateOpened}
}

func NewConnectionClosed(conn *Connection, err error) *ConnectionStateChangedEvent {
	return &ConnectionStateChangedEvent{
		Conn:  conn,
		Err:   err,
		state: ConnectionStateClosed,
	}
}

func (c ConnectionStateChangedEvent) EventName() string {
	return EventConnection
}

type MembersState int

const (
	MembersStateAdded MembersState = iota
	MembersStateRemoved
)

type MembersStateChangedEvent struct {
	Members []pubcluster.MemberInfo
	State   MembersState
}

func NewMembersAdded(members []pubcluster.MemberInfo) *MembersStateChangedEvent {
	return &MembersStateChangedEvent{Members: members, State: MembersStateAdded}
}

func (m MembersStateChangedEvent) EventName() string {
	return EventMembers
}

func NewMemberRemoved(members []pubcluster.MemberInfo) *MembersStateChangedEvent {
	return &MembersStateChangedEvent{Members: members, State: MembersStateRemoved}
}

type ClusterState int

const (
	ClusterStateConnected ClusterState = iota
	ClusterStateDisconnected
)

type ClusterStateChangedEvent struct {
	Addr  *pubcluster.Address
	State ClusterState
}

func NewConnected(addr *pubcluster.Address) *ClusterStateChangedEvent {
	return &ClusterStateChangedEvent{Addr: addr, State: ClusterStateConnected}
}

func (e *ClusterStateChangedEvent) EventName() string {
	return EventCluster
}

func NewDisconnected() *ClusterStateChangedEvent {
	return &ClusterStateChangedEvent{Addr: nil, State: ClusterStateDisconnected}
}
