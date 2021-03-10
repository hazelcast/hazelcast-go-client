package client_test

import (
	"github.com/hazelcast/hazelcast-go-client/v4/client"
	"testing"
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
	m, err := hz.GetMap("my-map")
	if err != nil {
		t.Error(err)
		return
	}
	targetValue := "value"
	if _, err := m.Put("key", targetValue); err != nil {
		t.Fatal(err)
	}
	if value, err := m.Get("key"); err != nil {
		t.Fatal(err)
		return
	} else if targetValue != value {
		t.Fatalf("target %v != %v", targetValue, value)
	}
}
