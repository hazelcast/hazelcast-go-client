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

const EnvDisableSmart = "DISABLE_SMART"
const EnvDisableNonsmart = "DISABLE_NONSMART"
const EnvTraceLogging = "ENABLE_TRACE"

func GetClientMapWithConfigBuilder(name string, configBuilder *hz.ConfigBuilder) (*hz.Client, hztypes.Map) {
	if TraceLoggingEnabled() {
		configBuilder.Logger().SetLevel(logger.TraceLevel)
	}
	client, err := hz.StartNewClientWithConfig(configBuilder)
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
	if SmartEnabled() {
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
	if NonSmartEnabled() {
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

func TraceLoggingEnabled() bool {
	return os.Getenv(EnvTraceLogging) == "1"
}

func SmartEnabled() bool {
	return os.Getenv(EnvDisableSmart) != "1"
}

func NonSmartEnabled() bool {
	return os.Getenv(EnvDisableNonsmart) != "1"
}
