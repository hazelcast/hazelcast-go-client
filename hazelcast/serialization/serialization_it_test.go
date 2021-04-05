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
	cb.Serialization().AddPortableFactory(1, &portableFactory{})
	config, err := cb.Config()
	if err != nil {
		panic(err)
	}
	client, m := it.GetClientMapWithConfig("ser-map", config)
	defer func() {
		m.EvictAll()
		client.Shutdown()
	}()
	target := newEmployee("Ford Prefect", 33, true)
	hz.Must(m.Set("ford", target))
	if value, err := m.Get("ford"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(target, value) {
		t.Fatalf("target %#v: %#v", target, value)
	}
}

type employee struct {
	name   string
	age    int
	active bool
}

func newEmployee(name string, age int, active bool) *employee {
	return &employee{
		name:   name,
		age:    age,
		active: active,
	}
}

func (e *employee) ReadPortable(reader serialization.PortableReader) error {
	e.name = reader.ReadUTF("name")
	e.age = int(reader.ReadInt32("age"))
	e.active = reader.ReadBool("active")
	return reader.Error()
}

func (e *employee) WritePortable(writer serialization.PortableWriter) error {
	writer.WriteUTF("name", e.name)
	writer.WriteInt32("age", int32(e.age))
	writer.WriteBool("active", e.active)
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
