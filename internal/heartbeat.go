// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/go-client/internal/protocol"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DEFAULT_HEARTBEAT_INTERVAL = 10
	DEFAULT_HEARTBEAT_TIMEOUT  = 60
)

type HeartBeatService struct {
	client            *HazelcastClient
	heartBeatTimeout  time.Duration
	heartBeatInterval time.Duration
	cancel            chan struct{}
	listeners         atomic.Value
	mu                sync.Mutex
}

func newHeartBeatService(client *HazelcastClient) *HeartBeatService {
	heartBeat := HeartBeatService{client: client, heartBeatInterval: DEFAULT_HEARTBEAT_INTERVAL,
		heartBeatTimeout: DEFAULT_HEARTBEAT_TIMEOUT,
		cancel:           make(chan struct{}),
	}
	if client.ClientConfig.HeartbeatTimeout() > 0 {
		heartBeat.heartBeatTimeout = time.Duration(client.ClientConfig.HeartbeatTimeout())
	}
	if client.ClientConfig.HeartbeatInterval() > 0 {
		heartBeat.heartBeatInterval = time.Duration(client.ClientConfig.HeartbeatInterval())
	}
	heartBeat.listeners.Store(make([]interface{}, 0)) //initialize
	return &heartBeat
}
func (heartBeatService *HeartBeatService) AddHeartbeatListener(listener interface{}) {
	heartBeatService.mu.Lock() //To prevent other potential writers
	defer heartBeatService.mu.Unlock()
	listeners := heartBeatService.listeners.Load().([]interface{})
	newSize := len(listeners) + 1
	copyListeners := make([]interface{}, newSize)
	for index, listener := range listeners {
		copyListeners[index] = listener
	}
	copyListeners[newSize-1] = listener
	heartBeatService.listeners.Store(copyListeners)
}
func (heartBeat *HeartBeatService) start() {
	go func() {
		ticker := time.NewTicker(heartBeat.heartBeatInterval * time.Second)
		for {
			if !heartBeat.client.LifecycleService.isLive.Load().(bool) {
				return
			}
			select {
			case <-ticker.C:
				heartBeat.heartBeat()
			case <-heartBeat.cancel:
				ticker.Stop()
				return
			}
		}
	}()
}
func (heartBeat *HeartBeatService) heartBeat() {
	heartBeat.client.ConnectionManager.lock.RLock()
	defer heartBeat.client.ConnectionManager.lock.RUnlock()
	for _, connection := range heartBeat.client.ConnectionManager.connections {
		timeSinceLastRead := time.Since(connection.lastRead.Load().(time.Time))
		if time.Duration(timeSinceLastRead.Seconds()) > heartBeat.heartBeatTimeout {
			if connection.heartBeating {
				heartBeat.onHeartBeatStopped(connection)
			}
		}
		if time.Duration(timeSinceLastRead.Seconds()) > heartBeat.heartBeatInterval {
			connection.lastHeartbeatRequested.Store(time.Now())
			request := protocol.ClientPingEncodeRequest()
			sentInvocation := heartBeat.client.InvocationService.InvokeOnConnection(request, connection)
			go func() {
				_, err := sentInvocation.Result()
				if err != nil {
					log.Println("error receiving heartbeat for connection, ", connection)
				} else {
					connection.lastHeartbeatReceived.Store(time.Now())
				}
			}()
		} else {
			if !connection.heartBeating {
				heartBeat.onHeartBeatRestored(connection)
			}
		}
	}
}
func (heartBeat *HeartBeatService) onHeartBeatRestored(connection *Connection) {
	log.Println("Heartbeat restored for a connection ", connection)
	connection.heartBeating = true
	listeners := heartBeat.listeners.Load().([]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(IOnHeartbeatRestored); ok {
			listener.(IOnHeartbeatRestored).OnHeartbeatRestored(connection)
		}
	}
}
func (heartBeat *HeartBeatService) onHeartBeatStopped(connection *Connection) {
	log.Println("Heartbeat stopped for a connection ", connection)
	connection.heartBeating = false
	listeners := heartBeat.listeners.Load().([]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(IOnHeartbeatStopped); ok {
			listener.(IOnHeartbeatStopped).OnHeartbeatStopped(connection)
		}
	}
}
func (heartBeat *HeartBeatService) shutdown() {
	close(heartBeat.cancel)
}

type IOnHeartbeatStopped interface {
	OnHeartbeatStopped(connection *Connection)
}
type IOnHeartbeatRestored interface {
	OnHeartbeatRestored(connection *Connection)
}
