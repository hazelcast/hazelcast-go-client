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

	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
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
	heartBeat := heartBeatService{client: client,
		heartBeatInterval: client.properties.GetPositiveDuration(property.HeartbeatInterval),
		heartBeatTimeout:  client.properties.GetPositiveDuration(property.HeartbeatTimeout),
		cancel:            make(chan struct{}),
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
	copy(copyListeners, listeners)
	copyListeners[newSize-1] = listener
	hbs.listeners.Store(copyListeners)
}

func (hbs *heartBeatService) start() {
	go func() {
		ticker := time.NewTicker(hbs.heartBeatInterval)
		for {
			if !hbs.client.lifecycleService.isLive.Load().(bool) {
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
				hbs.HeartbeatStopped(connection)
			}
		}
		if timeSinceLastRead > hbs.heartBeatInterval {
			connection.lastHeartbeatRequested.Store(time.Now())
			request := proto.ClientPingEncodeRequest()
			sentInvocation := hbs.client.InvocationService.invokeOnConnection(request, connection)
			copyConnection := connection
			go func() {
				_, err := sentInvocation.Result()
				if err != nil {
					log.Println("Error when receiving heartbeat for connection, ", copyConnection)
				} else {
					copyConnection.lastHeartbeatReceived.Store(time.Now())
				}
			}()
		} else {
			if !connection.heartBeating {
				hbs.HeartbeatResumed(connection)
			}
		}
	}
}

func (hbs *heartBeatService) HeartbeatResumed(connection *Connection) {
	log.Println("Heartbeat restored for a connection ", connection)
	connection.heartBeating = true
	listeners := hbs.listeners.Load().([]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(ConnectionHeartbeatListener); ok {
			listener.(ConnectionHeartbeatListener).HeartbeatResumed(connection)
		}
	}
}

func (hbs *heartBeatService) HeartbeatStopped(connection *Connection) {
	log.Println("Heartbeat stopped for a connection ", connection)
	connection.heartBeating = false
	listeners := hbs.listeners.Load().([]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(ConnectionHeartbeatListener); ok {
			listener.(ConnectionHeartbeatListener).HeartbeatStopped(connection)
		}
	}
}

func (hbs *heartBeatService) shutdown() {
	close(hbs.cancel)
}

type ConnectionHeartbeatListener interface {
	HeartbeatStopped(connection *Connection)
	HeartbeatResumed(connection *Connection)
}
