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
