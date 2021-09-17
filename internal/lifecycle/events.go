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

package lifecycle

// InternalLifecycleState indicates the state of the lifecycle event.
type InternalLifecycleState int

const (
	// InternalLifecycleStateStarting signals that the client is starting.
	InternalLifecycleStateStarting InternalLifecycleState = iota
	// InternalLifecycleStateStarted signals that the client started.
	InternalLifecycleStateStarted
	// InternalLifecycleStateShuttingDown signals that the client is shutting down.
	InternalLifecycleStateShuttingDown
	// InternalLifecycleStateShutDown signals that the client shut down.
	InternalLifecycleStateShutDown
	// InternalLifecycleStateConnected signals that the client connected to the cluster.
	InternalLifecycleStateConnected
	// InternalLifecycleStateDisconnected signals that the client disconnected from the cluster.
	InternalLifecycleStateDisconnected
	// InternalLifecycleStateChangedCluster signals that the client is connected to a new cluster.
	InternalLifecycleStateChangedCluster
)
const (
	// EventLifecycleEventStateChanged is dispatched for client lifecycle change events
	EventLifecycleEventStateChanged = "lifecyclestatechanged"
)

// InternalLifecycleStateChanged contains information about a lifecycle event.
type InternalLifecycleStateChanged struct {
	State InternalLifecycleState
}

func (e *InternalLifecycleStateChanged) EventName() string {
	return EventLifecycleEventStateChanged
}

func NewLifecycleStateChanged(state InternalLifecycleState) *InternalLifecycleStateChanged {
	return &InternalLifecycleStateChanged{State: state}
}
