package lifecycle

import publifecycle "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"

func NewStateChanged(state publifecycle.State) *publifecycle.StateChanged {
	return &publifecycle.StateChanged{State: state}
}
