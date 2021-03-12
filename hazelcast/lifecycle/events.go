package lifecycle

import "github.com/hazelcast/hazelcast-go-client/v4/internal/event"

type StateChanged interface {
	event.Event
	State() State
}

type StateChangeHandler func(event StateChanged)
