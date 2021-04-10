package it

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

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
		if cbCallback != nil {
			cbCallback(cb)
		}
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
		// TODO: remove the following sleep once we dynamically add connection listeners
		time.Sleep(2 * time.Second)
		f(t, m)

	})
	t.Run("Non-Smart Client", func(t *testing.T) {
		cb := hz.NewConfigBuilder()
		if cbCallback != nil {
			cbCallback(cb)
		}
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

func ClientTesterWithConfigBuilder(t *testing.T, cbCallback func(cb *hz.ConfigBuilder), f func(t *testing.T, c *hz.Client)) {
	t.Run("Smart Client", func(t *testing.T) {
		cb := hz.NewConfigBuilder()
		if cbCallback != nil {
			cbCallback(cb)
		}
		cb.SetProperty(property.LoggingLevel, "trace")
		config, err := cb.Config()
		if err != nil {
			panic(err)
		}
		client, err := hz.StartNewClientWithConfig(config)
		if err != nil {
			panic(err)
		}
		defer client.Shutdown()
		f(t, client)
	})
	t.Run("Non-Smart Client", func(t *testing.T) {
		cb := hz.NewConfigBuilder()
		if cbCallback != nil {
			cbCallback(cb)
		}
		cb.SetProperty(property.LoggingLevel, "trace")
		cb.Cluster().SetSmartRouting(false)
		config, err := cb.Config()
		if err != nil {
			panic(err)
		}
		client, err := hz.StartNewClientWithConfig(config)
		if err != nil {
			panic(err)
		}
		defer client.Shutdown()
		f(t, client)
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

func SamplePortableFromJSONValue(value hztypes.JSONValue) SamplePortable {
	sample := SamplePortable{}
	if err := json.Unmarshal(value, &sample); err != nil {
		panic(err)
	}
	return sample
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
	s.A = reader.ReadString("A")
	s.B = reader.ReadInt32("B")
	return nil
}

func (s SamplePortable) JSONValue() hztypes.JSONValue {
	byteArr, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return byteArr
}

type SamplePortableFactory struct {
}

func (f SamplePortableFactory) Create(classID int32) serialization.Portable {
	if classID == SamplePortableClassID {
		return &SamplePortable{}
	}
	return nil
}

func (f SamplePortableFactory) FactoryID() int32 {
	return SamplePortableFactoryID
}
