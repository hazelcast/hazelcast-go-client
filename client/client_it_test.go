package client_test

import (
	"github.com/hazelcast/hazelcast-go-client/v4/client"
	"sync"
	"testing"
)

func TestNewClientGetMap(t *testing.T) {
	hz := client.NewClient()
	if hz == nil {
		t.Fatalf("client is nil")
	}
	if err := hz.Start(); err != nil {
		t.Fatal(err)
	}
	m, err := hz.GetMap("my-map")
	if err != nil {
		t.Fatal(err)
	}
	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			targetValue := "value"
			if _, err := m.Put("key", targetValue); err != nil {
				t.Fatal(err)
			}
			if value, err := m.Get("key"); err != nil {
				t.Fatal(err)
			} else if targetValue != value {
				t.Fatalf("target %v != %v", targetValue, value)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
