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

// State indicates the state of the lifecycle event.
type State int

const (
	// StateStarting signals that the client is starting.
	StateStarting State = iota
	// StateStarted signals that the client started.
	StateStarted
	// StateShuttingDown signals that the client is shutting down.
	StateShuttingDown
	// StateShutDown signals that the client shut down.
	StateShutDown
	// StateConnected signals that the client connected to the cluster.
	StateConnected
	// StateDisconnected signals that the client disconnected from the cluster.
	StateDisconnected
	// StateChangedCluster signals that the client is connected to a new cluster.
	StateChangedCluster
)
const (
	// EventLifecycleStateChanged is dispatched for client lifecycle change events
	EventLifecycleStateChanged = "lifecyclestatechanged"
)

// StateChangedEvent contains information about a lifecycle event.
type StateChangedEvent struct {
	State State
}

func (e *StateChangedEvent) EventName() string {
	return EventLifecycleStateChanged
}

func NewLifecycleStateChanged(state State) *StateChangedEvent {
	return &StateChangedEvent{State: state}
}
