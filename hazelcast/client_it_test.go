package hazelcast_test

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"testing"
	"time"
)

func TestNewClientGetMap(t *testing.T) {
	client := hazelcast.NewClient()
	if client == nil {
		t.Errorf("client is nil")
		return
	}
	if err := client.Start(); err != nil {
		t.Error(err)
		return
	}
	time.Sleep(2 * time.Second)
	m, err := client.GetMap("my-map")
	if err != nil {
		t.Error(err)
		return
	}
	value, err := m.Get("foo")
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(value)
}
