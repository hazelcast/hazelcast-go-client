// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"log"
	"sync"
	"sync/atomic"
)

const (
	LIFECYCLE_STATE_STARTING      = "STARTING"
	LIFECYCLE_STATE_STARTED       = "STARTED"
	LIFECYCLE_STATE_CONNECTED     = "CONNECTED"
	LIFECYCLE_STATE_DISCONNECTED  = "DISCONNECTED"
	LIFECYCLE_STATE_SHUTTING_DOWN = "SHUTTING_DOWN"
	LIFECYCLE_STATE_SHUTDOWN      = "SHUTDOWN"
)

type LifecycleService struct {
	isLive    atomic.Value
	listeners atomic.Value
	mu        sync.Mutex
}

func newLifecycleService(config *config.ClientConfig) *LifecycleService {
	newLifecycle := &LifecycleService{}
	newLifecycle.isLive.Store(true)
	newLifecycle.listeners.Store(make(map[string]interface{})) //Initialize
	for _, listener := range config.LifecycleListeners() {
		if _, ok := listener.(core.ILifecycleListener); ok {
			newLifecycle.AddListener(listener)
		}
	}
	newLifecycle.fireLifecycleEvent(LIFECYCLE_STATE_STARTING)
	return newLifecycle
}
func (lifecycleService *LifecycleService) AddListener(listener interface{}) string {
	registrationId, _ := common.NewUUID()
	lifecycleService.mu.Lock()
	defer lifecycleService.mu.Unlock()
	listeners := lifecycleService.listeners.Load().(map[string]interface{})
	copyListeners := make(map[string]interface{}, len(listeners)+1)
	for k, v := range listeners {
		copyListeners[k] = v
	}
	copyListeners[registrationId] = listener
	lifecycleService.listeners.Store(copyListeners)
	return registrationId
}
func (lifecycleService *LifecycleService) RemoveListener(registrationId *string) bool {
	lifecycleService.mu.Lock()
	defer lifecycleService.mu.Unlock()
	listeners := lifecycleService.listeners.Load().(map[string]interface{})
	copyListeners := make(map[string]interface{}, len(listeners)-1)
	for k, v := range listeners {
		copyListeners[k] = v
	}
	_, found := copyListeners[*registrationId]
	if found {
		delete(copyListeners, *registrationId)
	}
	lifecycleService.listeners.Store(copyListeners)
	return found
}
func (lifecycleService *LifecycleService) fireLifecycleEvent(newState string) {
	if newState == LIFECYCLE_STATE_SHUTTING_DOWN {
		lifecycleService.isLive.Store(false)
	}
	listeners := lifecycleService.listeners.Load().(map[string]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(core.ILifecycleListener); ok {
			listener.(core.ILifecycleListener).LifecycleStateChanged(newState)
		}
	}
	log.Println("New State : ", newState)
}
