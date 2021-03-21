package hazelcast_test

import (
	"errors"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"testing"
)

func TestMustSucceeds(t *testing.T) {
	f := func() error {
		return nil
	}
	hazelcast.Must(f())
}

func TestMustFails(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Fatal("should have failed")
		}
	}()
	f := func() error {
		return errors.New("some error")
	}
	hazelcast.Must(f())
}

func TestMustValueSucceeds(t *testing.T) {
	targetValue := "OK"
	f := func() (interface{}, error) {
		return targetValue, nil
	}
	if val := hazelcast.MustValue(f()); targetValue != val {
		t.Fatalf("target %v != %v", targetValue, val)
	}
}

func TestMustValueFails(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Fatal("should have failed")
		}
	}()
	f := func() (interface{}, error) {
		return nil, errors.New("some error")
	}
	hazelcast.MustValue(f())
}

func TestMustBoolSucceeds(t *testing.T) {
	targetValue := true
	f := func() (bool, error) {
		return targetValue, nil
	}
	if val := hazelcast.MustBool(f()); !val {
		t.Fatalf("target %v != %v", true, val)
	}
}

func TestMustBoolFails(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Fatal("should have failed")
		}
	}()
	f := func() (bool, error) {
		return false, errors.New("some error")
	}
	hazelcast.MustValue(f())
}
