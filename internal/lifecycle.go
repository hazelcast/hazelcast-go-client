package internal

import (
	"fmt"
	"github.com/hazelcast/go-client/config"
)

const (
	LIFECYCLE_STATE_STARTING      = "STARTING"
	LIFECYCLE_STATE_CONNECTED     = "CONNECTED"
	LIFECYCLE_STATE_DISCONNECTED  = "DISCONNECTED"
	LIFECYCLE_STATE_SHUTTING_DOWN = "SHUTTING_DOWN"
	LIFECYCLE_STATE_SHUTDOWN      = "SHUTDOWN"
)

type LifecycleService struct {
	isLive bool
	state  string
}

func newLifecycleService(config *config.ClientConfig) *LifecycleService {
	newLifecycle := &LifecycleService{isLive: true}
	newLifecycle.fireLifecycleEvent(LIFECYCLE_STATE_STARTING)
	return newLifecycle
}
func (lifecycleService *LifecycleService) fireLifecycleEvent(newState string) {
	if newState == LIFECYCLE_STATE_SHUTTING_DOWN {
		lifecycleService.isLive = false
	}
	lifecycleService.state = newState
	fmt.Println("New State : ", lifecycleService.state)
}
