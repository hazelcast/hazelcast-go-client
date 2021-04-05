package it

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"

	hz "github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/property"
)

func GetClientMap(name string) (*hz.Client, hztypes.Map) {
	cb := hz.NewClientConfigBuilder()
	cb.SetProperty(property.LoggingLevel, "trace")
	config, err := cb.Config()
	if err != nil {
		panic(err)
	}
	client, err := hz.StartNewClientWithConfig(config)
	if err != nil {
		panic(err)
	}
	mapName := fmt.Sprintf("%s-%d", name, rand.Int())
	fmt.Println("Map Name:", mapName)
	if m, err := client.GetMap(mapName); err != nil {
		panic(err)
	} else {
		return client, m
	}
}

func GetClientMapWithConfig(name string, clientConfig hz.Config) (*hz.Client, hztypes.Map) {
	clientConfig.Properties[property.LoggingLevel] = "trace"
	client, err := hz.StartNewClientWithConfig(clientConfig)
	if err != nil {
		panic(err)
	}
	mapName := fmt.Sprintf("%s-%d", name, rand.Int())
	fmt.Println("Map Name:", mapName)
	if m, err := client.GetMap(mapName); err != nil {
		panic(err)
		return nil, nil
	} else {
		return client, m
	}
}

func GetClientMapSmart(name string) (*hz.Client, hztypes.Map) {
	return GetClientMap(name)
}

func GetClientMapNonSmart(name string) (*hz.Client, hztypes.Map) {
	cb := hz.NewClientConfigBuilder()
	cb.Cluster().SetSmartRouting(false)
	if config, err := cb.Config(); err != nil {
		panic(err)
	} else {
		return GetClientMapWithConfig(name, config)
	}
}

func MapTester(t *testing.T, f func(t *testing.T, m hztypes.Map)) {
	cbCallback := func(cb *hz.ConfigBuilder) {
	}
	MapTesterWithConfigBuilder(t, cbCallback, f)
}

func MapTesterWithConfigBuilder(t *testing.T, cbCallback func(cb *hz.ConfigBuilder), f func(t *testing.T, m hztypes.Map)) {
	var (
		client *hz.Client
		m      hztypes.Map
	)
	t.Run("Smart Client", func(t *testing.T) {
		cb := hz.NewConfigBuilder()
		cbCallback(cb)
		config, err := cb.Config()
		if err != nil {
			panic(err)
		}
		client, m = GetClientMapWithConfig("my-map", config)
		defer func() {
			if err := m.EvictAll(); err != nil {
				panic(err)
			}
			client.Shutdown()
		}()
		f(t, m)

	})
	t.Run("Non-Smart Client", func(t *testing.T) {
		cb := hz.NewConfigBuilder()
		cbCallback(cb)
		cb.Cluster().SetSmartRouting(false)
		config, err := cb.Config()
		if err != nil {
			panic(err)
		}
		config.ClusterConfig.SmartRouting = false
		client, m = GetClientMapWithConfig("my-map", config)
		defer func() {
			if err := m.EvictAll(); err != nil {
				panic(err)
			}
			client.Shutdown()
		}()
		f(t, m)
	})
}

func AssertEquals(t *testing.T, target, value interface{}) {
	if !reflect.DeepEqual(target, value) {
		t.Fatalf("target: %#v != %#v", target, value)
	}
}

const SamplePortableFactoryID = 1
const SamplePortableClassID = 1

type SamplePortable struct {
	A string
	B int32
}

func (s SamplePortable) FactoryID() int32 {
	return SamplePortableFactoryID
}

func (s SamplePortable) ClassID() int32 {
	return SamplePortableClassID
}

func (s SamplePortable) WritePortable(writer serialization.PortableWriter) error {
	writer.WriteUTF("A", s.A)
	writer.WriteInt32("B", s.B)
	return nil
}

func (s *SamplePortable) ReadPortable(reader serialization.PortableReader) error {
	s.A = reader.ReadUTF("A")
	s.B = reader.ReadInt32("B")
	return nil
}

type SamplePortableFactory struct {
}

func (f SamplePortableFactory) Create(classID int32) serialization.Portable {
	if classID == SamplePortableClassID {
		return &SamplePortable{}
	}
	return nil
}
