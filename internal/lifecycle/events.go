package lifecycle

import "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"

const EventStateChanged = "internal.lifecycle.statechanged"

type StateChangedImpl struct {
	state lifecycle.State
}

func NewStateChangedImpl(state lifecycle.State) *StateChangedImpl {
	return &StateChangedImpl{state: state}
}

func (e *StateChangedImpl) Name() string {
	return EventStateChanged
}
