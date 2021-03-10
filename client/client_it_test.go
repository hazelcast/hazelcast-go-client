package client_test

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/client"
	"testing"
	"time"
)

func TestNewClientGetMap(t *testing.T) {
	hz := client.NewClient()
	if hz == nil {
		t.Errorf("client is nil")
		return
	}
	if err := hz.Start(); err != nil {
		t.Error(err)
		return
	}
	time.Sleep(2 * time.Second)
	m, err := hz.GetMap("my-map")
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
