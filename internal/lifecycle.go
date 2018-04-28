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
	"log"
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/IPutil"
)

const (
	LifecycleStateStarting     = "STARTING"
	LifecycleStateStarted      = "STARTED"
	LifecycleStateConnected    = "CONNECTED"
	LifecycleStateDisconnected = "DISCONNECTED"
	LifecycleStateShuttingDown = "SHUTTING_DOWN"
	LifecycleStateShutdown     = "SHUTDOWN"
)

type lifecycleService struct {
	isLive    atomic.Value
	listeners atomic.Value
	mu        sync.Mutex
}

func newLifecycleService(config *config.ClientConfig) *lifecycleService {
	newLifecycle := &lifecycleService{}
	newLifecycle.isLive.Store(true)
	newLifecycle.listeners.Store(make(map[string]interface{})) //Initialize
	for _, listener := range config.LifecycleListeners() {
		if _, ok := listener.(core.LifecycleListener); ok {
			newLifecycle.AddListener(listener)
		}
	}
	newLifecycle.fireLifecycleEvent(LifecycleStateStarting)
	return newLifecycle
}

func (ls *lifecycleService) AddListener(listener interface{}) string {
	registrationID, _ := IPutil.NewUUID()
	ls.mu.Lock()
	defer ls.mu.Unlock()
	listeners := ls.listeners.Load().(map[string]interface{})
	copyListeners := make(map[string]interface{}, len(listeners)+1)
	for k, v := range listeners {
		copyListeners[k] = v
	}
	copyListeners[registrationID] = listener
	ls.listeners.Store(copyListeners)
	return registrationID
}

func (ls *lifecycleService) RemoveListener(registrationID *string) bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	listeners := ls.listeners.Load().(map[string]interface{})
	copyListeners := make(map[string]interface{}, len(listeners)-1)
	for k, v := range listeners {
		copyListeners[k] = v
	}
	_, found := copyListeners[*registrationID]
	if found {
		delete(copyListeners, *registrationID)
	}
	ls.listeners.Store(copyListeners)
	return found
}

func (ls *lifecycleService) fireLifecycleEvent(newState string) {
	if newState == LifecycleStateShuttingDown {
		ls.isLive.Store(false)
	}
	listeners := ls.listeners.Load().(map[string]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(core.LifecycleListener); ok {
			listener.(core.LifecycleListener).LifecycleStateChanged(newState)
		}
	}
	log.Println("New State : ", newState)
}
