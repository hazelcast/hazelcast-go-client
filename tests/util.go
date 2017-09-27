package tests

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

var Timeout time.Duration = 2 * time.Minute

func assertEqualf(t *testing.T, err error, l interface{}, r interface{}, message string) {
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
func assertEqual(t *testing.T, err error, l interface{}, r interface{}) {
	if err != nil {
		t.Fatal(err)
	}
	if l != r {
		t.Fatalf("%v != %v", l, r)
	}

}
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
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
