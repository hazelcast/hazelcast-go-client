package lifecycle

import publifecycle "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"

func NewStateChangedImpl(state publifecycle.State) *publifecycle.StateChanged {
	return &publifecycle.StateChanged{State: state}
}
