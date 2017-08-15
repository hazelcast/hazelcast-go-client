package internal

import (
	"fmt"
	"github.com/hazelcast/go-client/internal/protocol"
	"time"
)

const (
	DEFAULT_HEARTBEAT_INTERVAL = 5
	DEFAULT_HEARTBEAT_TIMEOUT  = 60
)

type HeartBeatService struct {
	client            *HazelcastClient
	heartBeatTimeout  float64
	heartBeatInterval float64
	alive             chan bool
	cancel            chan bool
}

func newHeartBeatService(client *HazelcastClient) *HeartBeatService {
	//TODO:: Add listeners
	heartBeat := HeartBeatService{client: client, heartBeatInterval: DEFAULT_HEARTBEAT_INTERVAL, heartBeatTimeout: DEFAULT_HEARTBEAT_TIMEOUT,
		alive: make(chan bool, 0), cancel: make(chan bool, 0),
	}
	return &heartBeat
}
func (heartBeat *HeartBeatService) start() {
	go func() {
		for {
			if !heartBeat.client.LifecycleService.isLive {
				return
			}
			select {
			case <-heartBeat.alive:
				go heartBeat.heartBeat()
				fmt.Println(time.Second.Seconds() * heartBeat.heartBeatInterval)
				time.Sleep(time.Duration(time.Second.Seconds()*heartBeat.heartBeatInterval) * time.Second)
			case <-heartBeat.cancel:
				return
			}
		}
	}()
	heartBeat.alive <- true //To start the heartbeat.
}
func (heartBeat *HeartBeatService) heartBeat() {
	for _, connection := range heartBeat.client.ConnectionManager.connections {
		timeSinceLastRead := time.Since(connection.lastRead)
		if timeSinceLastRead.Seconds() > heartBeat.heartBeatTimeout {
			if connection.heartBeating {

				fmt.Println("Didnt hear back from a connection")
				heartBeat.onHeartBeatStop(connection)
			}
		}
		if timeSinceLastRead.Seconds() > heartBeat.heartBeatInterval {
			request := protocol.ClientPingEncodeRequest()
			heartBeat.client.InvocationService.InvokeOnConnection(request, connection)
		} else {
			if !connection.heartBeating {
				heartBeat.onHeartBeatRestored(connection)
			}
		}
	}
	heartBeat.alive <- true
}
func (heartBeat *HeartBeatService) onHeartBeatRestored(connection *Connection) {
	fmt.Println("Heartbeat restored for a connection")
	connection.heartBeating = true
}
func (heartBeat *HeartBeatService) onHeartBeatStop(connection *Connection) {
	connection.heartBeating = false
}
func (heartBeat *HeartBeatService) shutdown() {
	go func() {
		heartBeat.cancel <- true
	}()
}
