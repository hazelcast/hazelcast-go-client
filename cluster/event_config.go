package cluster

const (
	// Default values differ from java impl. Also queue size is calculated differently.
	// Java Client: queueSize per worker = defaultEventQueueCapacity / defaultEventWorkerCount
	// Go Client: queueSize per worker = defaultEventQueueCapacity
	defaultEventQueueCapacity = 10000
	defaultEventWorkerCount   = 5
)

type EventConfig struct {
	// EventWorkerCount is the number of the workers (goroutines) to handle the incoming event packets.
	// Default is 5
	EventWorkerCount uint32 `json:",omitempty"`
	// EventQueueCapacity is the capacity of the workers that handles the incoming event packets.
	// Default is 1000000
	EventQueueCapacity uint32 `json:",omitempty"`
}

func (ec EventConfig) Clone() EventConfig {
	return ec
}

func (ec *EventConfig) Validate() error {
	// set default values if needed
	if ec.EventWorkerCount == 0 {
		ec.EventWorkerCount = defaultEventWorkerCount
	}
	if ec.EventQueueCapacity == 0 {
		ec.EventQueueCapacity = defaultEventQueueCapacity
	}
	return nil
}
