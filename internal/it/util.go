package it

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/hztypes"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const ENV_DISABLE_SMART = "DISABLE_SMART"
const ENV_DISABLE_NONSMART = "DISABLE_NONSMART"

func GetClientMap(name string) (*hz.Client, hztypes.Map) {
	cb := hz.NewConfigBuilder()
	cb.Logger().SetLevel(logger.TraceLevel)
	client, err := hz.StartNewClientWithConfig(cb)
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

func GetClientMapWithConfigBuilder(name string, configBuilder *hz.ConfigBuilder) (*hz.Client, hztypes.Map) {
	configBuilder.Logger().SetLevel(logger.TraceLevel)
	client, err := hz.StartNewClientWithConfig(configBuilder)
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
	hasSmart := os.Getenv(ENV_DISABLE_SMART) != "1"
	hasNonSmart := os.Getenv(ENV_DISABLE_NONSMART) != "1"
	if hasSmart {
		t.Run("Smart Client", func(t *testing.T) {
			cb := hz.NewConfigBuilder()
			if cbCallback != nil {
				cbCallback(cb)
			}
			client, m = GetClientMapWithConfigBuilder("my-map", cb)
			defer func() {
				if err := m.EvictAll(); err != nil {
					panic(err)
				}
				client.Shutdown()
			}()
			f(t, m)
		})
	}
	if hasNonSmart {
		t.Run("Non-Smart Client", func(t *testing.T) {
			cb := hz.NewConfigBuilder()
			if cbCallback != nil {
				cbCallback(cb)
			}
			cb.Cluster().SetSmartRouting(false)
			client, m = GetClientMapWithConfigBuilder("my-map", cb)
			defer func() {
				if err := m.EvictAll(); err != nil {
					panic(err)
				}
				client.Shutdown()
			}()
			f(t, m)
		})
	}
}

func ClientTesterWithConfigBuilder(t *testing.T, cbCallback func(cb *hz.ConfigBuilder), f func(t *testing.T, c *hz.Client)) {
	t.Run("Smart Client", func(t *testing.T) {
		cb := hz.NewConfigBuilder()
		if cbCallback != nil {
			cbCallback(cb)
		}
		cb.Logger().SetLevel(logger.TraceLevel)
		client, err := hz.StartNewClientWithConfig(cb)
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
		cb.Logger().SetLevel(logger.TraceLevel)
		cb.Cluster().SetSmartRouting(false)
		client, err := hz.StartNewClientWithConfig(cb)
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

// Must panics if err is not nil
func Must(err error) {
	if err != nil {
		panic(err)
	}
}

// MustValue returns value if err is nil, otherwise it panics.
func MustValue(value interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return value
}

// MustBool returns value if err is nil, otherwise it panics.
func MustBool(value bool, err error) bool {
	if err != nil {
		panic(err)
	}
	return value
}

// MustClient returns client if err is nil, otherwise it panics.
func MustClient(client *hz.Client, err error) *hz.Client {
	if err != nil {
		panic(err)
	}
	return client
}

type trivialConfigProvider struct {
	config *hz.Config
}

func (p trivialConfigProvider) Config() (*hz.Config, error) {
	return p.config, nil
}
