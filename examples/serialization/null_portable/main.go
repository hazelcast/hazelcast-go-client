package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	factoryID   = 1
	fooClassID  = 1
	baseClassID = 2
)

type Foo struct {
	Bar string
}

func (Foo) FactoryID() int32 {
	return factoryID
}

func (Foo) ClassID() int32 {
	return fooClassID
}

func (f Foo) WritePortable(w serialization.PortableWriter) {
	w.WriteString("b", f.Bar)
}

func (f *Foo) ReadPortable(r serialization.PortableReader) {
	f.Bar = r.ReadString("b")
}

type BaseObject struct {
	Foo *Foo
}

func (BaseObject) FactoryID() int32 {
	return factoryID
}

func (BaseObject) ClassID() int32 {
	return baseClassID
}

func (b BaseObject) WritePortable(w serialization.PortableWriter) {
	if b.Foo == nil {
		w.WriteNilPortable("foo", factoryID, fooClassID)
		return
	}
	w.WritePortable("foo", b.Foo)
}

func (b *BaseObject) ReadPortable(r serialization.PortableReader) {
	v := r.ReadPortable("foo")
	if v != nil {
		b.Foo = v.(*Foo)
	}
}

type MyPortableFactory struct {
}

func (m MyPortableFactory) Create(classID int32) serialization.Portable {
	switch classID {
	case fooClassID:
		return &Foo{}
	case baseClassID:
		return &BaseObject{}
	}
	return nil
}

func (m MyPortableFactory) FactoryID() int32 {
	return factoryID
}

func main() {
	// create the configuration
	config := hazelcast.Config{}
	config.Serialization.SetPortableFactories(&MyPortableFactory{})
	classDefinition := serialization.NewClassDefinition(1, 1, 0)
	if err := classDefinition.AddStringField("b"); err != nil {
		log.Fatal(err)
	}
	config.Serialization.SetClassDefinitions(classDefinition)
	// start the client with the given configuration
	ctx := context.TODO()
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	// retrieve a map
	m, err := client.GetMap(ctx, "example")
	if err != nil {
		log.Fatal(err)
	}
	baseObj := &BaseObject{Foo: nil}
	if err := m.Set(ctx, "base-obj", baseObj); err != nil {
		log.Fatal(err)
	}
	if v, err := m.Get(ctx, "base-obj"); err != nil {
		log.Fatal(err)
	} else {
		fmt.Println(v)
	}
	// stop the client
	time.Sleep(1 * time.Second)
	client.Shutdown(ctx)
}
