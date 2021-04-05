package lifecycle

type State int

const (
	StateStarting State = iota
	StateStarted
	StateShuttingDown
	StateShutDown
	StateMerging
	StateMerged
	StateMergeFailed
	StateClientConnected
	StateClientDisconnected
	StateClientChangedCluster
)

type StateChangeHandler func(event StateChanged)

const EventStateChanged = "internal.lifecycle.statechanged"

type StateChanged struct {
	State State
}

func (e *StateChanged) EventName() string {
	return EventStateChanged
}
