package tests

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

var Timeout time.Duration = 1 * time.Minute

const DEFAULT_XML_CONFIG string = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config hazelcast-config-3.9.xsd\" xmlns=\"http://www.hazelcast.com/schema/config\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"></hazelcast>"

func AssertEqualf(t *testing.T, err error, l interface{}, r interface{}, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if l != r {
		t.Fatalf("%v != %v : %v", l, r, message)
	}
}
func AssertNilf(t *testing.T, err error, l interface{}, message string) {
	if !reflect.ValueOf(l).IsNil() {
		t.Fatalf("%v != nil", l)
	}
}

func AssertEqual(t *testing.T, err error, l interface{}, r interface{}) {
	if err != nil {
		t.Fatal(err)
	}
	if l != r {
		t.Fatalf("%v != %v", l, r)
	}

}
func WaitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
