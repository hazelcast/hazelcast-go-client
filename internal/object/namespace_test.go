package object

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestNewNamespaceImpl(t *testing.T) {
	ser := &mockIdentifiedDataSerializer{}
	ns := NewNamespaceImpl("hz:impl:mapService", "my-map")
	if err := ns.WriteData(ser); err != nil {
		t.Error(err)
		return
	}
	targetData := []interface{}{"hz:impl:mapService", "my-map"}
	if !reflect.DeepEqual(targetData, ser.data) {
		t.Errorf("target: %v != %v", targetData, ser.data)
		return
	}
	ser.rewind()
	ns2 := NamespaceImpl{}
	if err := ns2.ReadData(ser); err != nil {
		t.Error(err)
		return
	}
	targetServiceName := "hz:impl:mapService"
	targetObjectName := "my-map"
	if targetServiceName != ns2.ServiceName {
		t.Errorf("target: %v != %v", targetServiceName, ns2.ServiceName)
		return
	}
	if targetObjectName != ns2.ObjectName {
		t.Errorf("target: %v != %v", targetObjectName, ns2.ObjectName)
	}
}

type mockIdentifiedDataSerializer struct {
	data   []interface{}
	offset int
}

func (ser *mockIdentifiedDataSerializer) ReadString(dataOut *string) error {
	if dataOut == nil {
		panic("dataOut cannot be nil")
	}
	if ser.offset >= len(ser.data) {
		return errors.New("data unserializer overflow")
	}
	if data, ok := ser.data[ser.offset].(string); ok {
		ser.offset++
		*dataOut = data
		return nil
	}
	return fmt.Errorf("no string data at offset: %d", ser.offset)
}

func (ser *mockIdentifiedDataSerializer) WriteString(data string) error {
	// Assumes offset cannot be > len(data)
	if ser.offset >= len(ser.data) {
		ser.data = append(ser.data, data)
	} else {
		ser.data[ser.offset] = data
	}
	ser.offset++
	return nil
}

func (ser *mockIdentifiedDataSerializer) rewind() {
	ser.offset = 0
}
