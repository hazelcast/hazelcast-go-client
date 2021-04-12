package serialization_test

import (
	"reflect"
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client/v4/hazelcast"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/it"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

func TestPortableSerialize(t *testing.T) {
	cb := hz.NewConfigBuilder()
	cb.Serialization().AddPortableFactory(&portableFactory{})
	client, m := it.GetClientMapWithConfigBuilder("ser-map", cb)
	defer func() {
		m.EvictAll()
		client.Shutdown()
	}()
	target := newEmployee("Ford Prefect", 33, true)
	it.Must(m.Set("ford", target))
	if value, err := m.Get("ford"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(target, value) {
		t.Fatalf("target %#v: %#v", target, value)
	}
}

type employee struct {
	Name   string
	Age    int
	Active bool
}

func newEmployee(name string, age int, active bool) *employee {
	return &employee{
		Name:   name,
		Age:    age,
		Active: active,
	}
}

func (e *employee) ReadPortable(reader serialization.PortableReader) error {
	e.Name = reader.ReadString("Name")
	e.Age = int(reader.ReadInt32("Age"))
	e.Active = reader.ReadBool("Active")
	return reader.Error()
}

func (e *employee) WritePortable(writer serialization.PortableWriter) error {
	writer.WriteUTF("Name", e.Name)
	writer.WriteInt32("Age", int32(e.Age))
	writer.WriteBool("Active", e.Active)
	return nil
}

func (e *employee) FactoryID() int32 {
	return 1
}

func (e *employee) ClassID() int32 {
	return 1
}

type portableFactory struct {
}

func (p portableFactory) Create(classID int32) serialization.Portable {
	return &employee{}
}

func (p portableFactory) FactoryID() int32 {
	return 1
}
