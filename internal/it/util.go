/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"runtime/debug"
	"strconv"
	"sync"
	"testing"

	"go.uber.org/goleak"

	"github.com/apache/thrift/lib/go/thrift"
	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const EnvDisableSmart = "DISABLE_SMART"
const EnvDisableNonsmart = "DISABLE_NONSMART"
const EnvEnableTraceLogging = "ENABLE_TRACE"
const EnvMemberCount = "MEMBER_COUNT"
const EnvEnableLeakCheck = "ENABLE_LEAKCHECK"

const xmlConfig = `
        <hazelcast xmlns="http://www.hazelcast.com/schema/config"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.hazelcast.com/schema/config
            http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
            <cluster-name>integration-test</cluster-name>
            <network>
               <port>7701</port>
            </network>
			<map name="test-map">
				<map-store enabled="true">
					<class-name>com.hazelcast.client.test.SampleMapStore</class-name>
				</map-store>
			</map>
        </hazelcast>
`

var rc *RemoteControllerClient
var rcMu = &sync.RWMutex{}
var defaultTestCluster *testCluster

func GetClientMapWithConfigBuilder(mapName string, cb *hz.ConfigBuilder) (*hz.Client, *hz.Map) {
	if TraceLoggingEnabled() {
		cb.Logger().SetLevel(logger.TraceLevel)
	} else {
		cb.Logger().SetLevel(logger.WarnLevel)
	}
	client, err := hz.StartNewClientWithConfig(cb)
	if err != nil {
		panic(err)
	}
	if m, err := client.GetMap(mapName); err != nil {
		panic(err)
	} else {
		return client, m
	}
}

func TesterWithConfigBuilder(t *testing.T, cbCallback func(cb *hz.ConfigBuilder), f func(t *testing.T, client *hz.Client)) {
	ensureRemoteController()
	runner := func(t *testing.T, smart bool) {
		if LeakCheckEnabled() {
			t.Logf("enabled leak check")
			defer goleak.VerifyNone(t)
		}
		cb := defaultTestCluster.configBuilder()
		if cbCallback != nil {
			cbCallback(cb)
		}
		if TraceLoggingEnabled() {
			cb.Logger().SetLevel(logger.TraceLevel)
		} else {
			cb.Logger().SetLevel(logger.WarnLevel)
		}
		cb.Cluster().SetSmartRouting(smart)
		client := MustClient(hz.StartNewClientWithConfig(cb))
		defer func() {
			if err := client.Shutdown(); err != nil {
				t.Logf("Test warning, client not shutdown: %s", err.Error())
			}
		}()
		f(t, client)
	}
	if SmartEnabled() {
		t.Run("Smart Client", func(t *testing.T) {
			runner(t, true)
		})
	}
	if NonSmartEnabled() {
		t.Run("Non-Smart Client", func(t *testing.T) {
			runner(t, false)
		})
	}
}

func MapTester(t *testing.T, f func(t *testing.T, m *hz.Map)) {
	cbCallback := func(cb *hz.ConfigBuilder) {
	}
	MapTesterWithConfigBuilder(t, cbCallback, f)
}

func MapTesterWithConfigBuilder(t *testing.T, cbCallback func(cb *hz.ConfigBuilder), f func(t *testing.T, m *hz.Map)) {
	mapName := fmt.Sprintf("test-map-%d", rand.Int())
	MapTesterWithConfigBuilderWithName(t, mapName, cbCallback, f)
}

func MapTesterWithConfigBuilderWithName(t *testing.T, mapName string, cbCallback func(cb *hz.ConfigBuilder), f func(t *testing.T, m *hz.Map)) {
	var (
		client *hz.Client
		m      *hz.Map
	)
	ensureRemoteController()
	runner := func(t *testing.T, smart bool) {
		if LeakCheckEnabled() {
			t.Logf("enabled leak check")
			defer goleak.VerifyNone(t)
		}
		cb := defaultTestCluster.configBuilder()
		if cbCallback != nil {
			cbCallback(cb)
		}
		cb.Cluster().SetSmartRouting(smart)
		client, m = GetClientMapWithConfigBuilder(mapName, cb)
		defer func() {
			m.EvictAll()
			if err := client.Shutdown(); err != nil {
				t.Logf("Test warning, client not shutdown: %s", err.Error())
			}
		}()
		f(t, m)
	}
	if SmartEnabled() {
		t.Run("Smart Client", func(t *testing.T) {
			runner(t, true)
		})
	}
	if NonSmartEnabled() {
		t.Run("Non-Smart Client", func(t *testing.T) {
			runner(t, false)
		})
	}
}

func ReplicatedMapTesterWithConfigBuilder(t *testing.T, cbCallback func(cb *hz.ConfigBuilder), f func(t *testing.T, m *hz.ReplicatedMap)) {
	mapName := fmt.Sprintf("test-map-%d", rand.Int())
	ReplicatedMapTesterWithConfigBuilderWithName(t, mapName, cbCallback, f)
}

func ReplicatedMapTesterWithConfigBuilderWithName(t *testing.T, mapName string, cbCallback func(cb *hz.ConfigBuilder), f func(t *testing.T, m *hz.ReplicatedMap)) {
	var (
		client *hz.Client
		m      *hz.ReplicatedMap
	)
	ensureRemoteController()
	runner := func(t *testing.T, smart bool) {
		if LeakCheckEnabled() {
			t.Logf("enabled leak check")
			defer goleak.VerifyNone(t)
		}
		cb := defaultTestCluster.configBuilder()
		if cbCallback != nil {
			cbCallback(cb)
		}
		cb.Cluster().SetSmartRouting(smart)
		client, m = getClientReplicatedMapWithConfig(mapName, cb)
		defer func() {
			m.Clear()
			client.Shutdown()
		}()
		f(t, m)
	}
	t.Run("Smart Client", func(t *testing.T) {
		runner(t, true)
	})
	t.Run("Non-Smart Client", func(t *testing.T) {
		runner(t, false)
	})
}

func getClientReplicatedMapWithConfig(name string, cb *hz.ConfigBuilder) (*hz.Client, *hz.ReplicatedMap) {
	if TraceLoggingEnabled() {
		cb.Logger().SetLevel(logger.TraceLevel)
	} else {
		cb.Logger().SetLevel(logger.WarnLevel)
	}
	client, err := hz.StartNewClientWithConfig(cb)
	if err != nil {
		panic(err)
	}
	if m, err := client.GetReplicatedMap(name); err != nil {
		panic(err)
	} else {
		return client, m
	}
}

func AssertEquals(t *testing.T, target, value interface{}) {
	if !reflect.DeepEqual(target, value) {
		t.Log(string(debug.Stack()))
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
	writer.WriteString("A", s.A)
	writer.WriteInt32("B", s.B)
	return nil
}

func (s *SamplePortable) ReadPortable(reader serialization.PortableReader) error {
	s.A = reader.ReadString("A")
	s.B = reader.ReadInt32("B")
	return nil
}

func (s SamplePortable) Json() serialization.JSON {
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

func TraceLoggingEnabled() bool {
	return os.Getenv(EnvEnableTraceLogging) == "1"
}

func SmartEnabled() bool {
	return os.Getenv(EnvDisableSmart) != "1"
}

func NonSmartEnabled() bool {
	return os.Getenv(EnvDisableNonsmart) != "1"
}

func LeakCheckEnabled() bool {
	return os.Getenv(EnvEnableLeakCheck) == "1"
}

func defaultMemberCount() int {
	if memberCountStr := os.Getenv(EnvMemberCount); memberCountStr != "" {
		if memberCount, err := strconv.Atoi(memberCountStr); err != nil {
			panic(err)
		} else {
			return memberCount
		}
	}
	return 1
}

func createRemoteController() *RemoteControllerClient {
	transport := MustValue(thrift.NewTSocketConf("localhost:9701", nil)).(*thrift.TSocket)
	bufferedTransport := thrift.NewTBufferedTransport(transport, 4096)
	protocol := thrift.NewTBinaryProtocolConf(bufferedTransport, nil)
	client := thrift.NewTStandardClient(protocol, protocol)
	rc := NewRemoteControllerClient(client)
	Must(transport.Open())
	return rc
}

func ensureRemoteController() *RemoteControllerClient {
	rcMu.Lock()
	defer rcMu.Unlock()
	if rc == nil {
		rc = createRemoteController()
		if ping, err := rc.Ping(context.Background()); err != nil {
			panic(err)
		} else if !ping {
			panic("remote controller not accesible")
		}
		defaultTestCluster = startNewCluster(rc, defaultMemberCount())
	}
	return rc
}

type testCluster struct {
	rc          *RemoteControllerClient
	clusterID   string
	memberUUIDs []string
}

func startNewCluster(rc *RemoteControllerClient, memberCount int) *testCluster {
	cluster := MustValue(rc.CreateClusterKeepClusterName(context.Background(), "4.1", xmlConfig)).(*Cluster)
	memberUUIDs := make([]string, 0, memberCount)
	for i := 0; i < memberCount; i++ {
		member := MustValue(rc.StartMember(context.Background(), cluster.ID)).(*Member)
		memberUUIDs = append(memberUUIDs, member.UUID)
	}
	return &testCluster{
		rc:          rc,
		clusterID:   cluster.ID,
		memberUUIDs: memberUUIDs,
	}
}

func (c testCluster) shutdown() {
	for _, memberUUID := range c.memberUUIDs {
		c.rc.ShutdownMember(context.Background(), c.clusterID, memberUUID)
	}
}

func (c testCluster) configBuilder() *hz.ConfigBuilder {
	cb := hz.NewConfigBuilder()
	cb.Cluster().
		SetName(c.clusterID).
		SetMembers("localhost:7701")
	return cb
}
