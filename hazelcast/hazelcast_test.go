package hazelcast_test

import (
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"testing"
)

func TestCreateJSONValueFromString(t *testing.T) {
	var jsonValue *hazelcast.JSONValue
	stringValue := `
		{
			"foo": 42
		}
	`
	jsonValue = hazelcast.CreateJSONValueFromString(stringValue)
	if stringValue != jsonValue.ToString() {
		t.Fatalf("%#v != %#v", stringValue, jsonValue.ToString())
	}
}
