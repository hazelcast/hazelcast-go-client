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
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

const (
	DefaultHeartBeatInterval = 10 * time.Second
	DefaultHeartBeatTimeout  = 60 * time.Second
)

type heartBeatService struct {
	client            *HazelcastClient
	heartBeatTimeout  time.Duration
	heartBeatInterval time.Duration
	cancel            chan struct{}
	listeners         atomic.Value
	mu                sync.Mutex
}

func newHeartBeatService(client *HazelcastClient) *heartBeatService {
	heartBeat := heartBeatService{client: client, heartBeatInterval: DefaultHeartBeatInterval,
		heartBeatTimeout: DefaultHeartBeatTimeout,
		cancel:           make(chan struct{}),
	}
	if client.ClientConfig.HeartbeatTimeout() > 0 {
		heartBeat.heartBeatTimeout = client.ClientConfig.HeartbeatTimeout()
	}
	if client.ClientConfig.HeartbeatInterval() > 0 {
		heartBeat.heartBeatInterval = client.ClientConfig.HeartbeatInterval()
	}
	heartBeat.listeners.Store(make([]interface{}, 0)) //initialize
	return &heartBeat
}
func (hbs *heartBeatService) AddHeartbeatListener(listener interface{}) {
	hbs.mu.Lock() //To prevent other potential writers
	defer hbs.mu.Unlock()
	listeners := hbs.listeners.Load().([]interface{})
	newSize := len(listeners) + 1
	copyListeners := make([]interface{}, newSize)
	for index, listener := range listeners {
		copyListeners[index] = listener
	}
	copyListeners[newSize-1] = listener
	hbs.listeners.Store(copyListeners)
}
func (hbs *heartBeatService) start() {
	go func() {
		ticker := time.NewTicker(hbs.heartBeatInterval)
		for {
			if !hbs.client.LifecycleService.isLive.Load().(bool) {
				return
			}
			select {
			case <-ticker.C:
				hbs.heartBeat()
			case <-hbs.cancel:
				ticker.Stop()
				return
			}
		}
	}()
}
func (hbs *heartBeatService) heartBeat() {
	for _, connection := range hbs.client.ConnectionManager.getActiveConnections() {
		timeSinceLastRead := time.Since(connection.lastRead.Load().(time.Time))
		if timeSinceLastRead > hbs.heartBeatTimeout {
			if connection.heartBeating {
				hbs.onHeartBeatStopped(connection)
			}
		}
		if timeSinceLastRead > hbs.heartBeatInterval {
			connection.lastHeartbeatRequested.Store(time.Now())
			request := protocol.ClientPingEncodeRequest()
			sentInvocation := hbs.client.InvocationService.invokeOnConnection(request, connection)
			copyConnection := connection
			go func() {
				_, err := sentInvocation.Result()
				if err != nil {
					log.Println("error receiving heartbeat for connection, ", copyConnection)
				} else {
					copyConnection.lastHeartbeatReceived.Store(time.Now())
				}
			}()
		} else {
			if !connection.heartBeating {
				hbs.onHeartBeatRestored(connection)
			}
		}
	}
}
func (hbs *heartBeatService) onHeartBeatRestored(connection *Connection) {
	log.Println("Heartbeat restored for a connection ", connection)
	connection.heartBeating = true
	listeners := hbs.listeners.Load().([]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(IOnHeartbeatRestored); ok {
			listener.(IOnHeartbeatRestored).OnHeartbeatRestored(connection)
		}
	}
}
func (hbs *heartBeatService) onHeartBeatStopped(connection *Connection) {
	log.Println("Heartbeat stopped for a connection ", connection)
	connection.heartBeating = false
	listeners := hbs.listeners.Load().([]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(IOnHeartbeatStopped); ok {
			listener.(IOnHeartbeatStopped).OnHeartbeatStopped(connection)
		}
	}
}
func (hbs *heartBeatService) shutdown() {
	close(hbs.cancel)
}

type IOnHeartbeatStopped interface {
	OnHeartbeatStopped(connection *Connection)
}
type IOnHeartbeatRestored interface {
	OnHeartbeatRestored(connection *Connection)
}
