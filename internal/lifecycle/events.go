package lifecycle

import publifecycle "github.com/hazelcast/hazelcast-go-client/lifecycle"

func NewStateChanged(state publifecycle.State) *publifecycle.StateChanged {
	return &publifecycle.StateChanged{State: state}
}
