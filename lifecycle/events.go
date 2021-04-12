package lifecycle

import (
	"github.com/hazelcast/hazelcast-go-client/internal"
)

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
	// StateClientConnected signals that the client connected to the cluster.
	StateClientConnected
	// StateClientDisconnected signals that the client disconnected from the cluster.
	StateClientDisconnected
)

type StateChangeHandler func(event StateChanged)

type StateChanged struct {
	State State
}

func (e *StateChanged) EventName() string {
	return internal.LifecycleEventStateChanged
}
