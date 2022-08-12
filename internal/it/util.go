/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"reflect"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	EnvDisableSmart       = "DISABLE_SMART"
	EnvDisableNonsmart    = "DISABLE_NONSMART"
	EnvEnableTraceLogging = "ENABLE_TRACE"
	EnvMemberCount        = "MEMBER_COUNT"
	EnvEnableLeakCheck    = "ENABLE_LEAKCHECK"
	EnvEnableSSL          = "ENABLE_SSL"
	EnvHzVersion          = "HZ_VERSION"
)

const DefaultClusterName = "integration-test"

var rc *RemoteControllerClientWrapper
var rcMu = &sync.RWMutex{}
var defaultTestCluster = NewSingletonTestCluster("default", func() *TestCluster {
	port := NextPort()
	if SSLEnabled() {
		return rc.startNewCluster(MemberCount(), xmlSSLConfig(DefaultClusterName, port), port)
	}
	return rc.startNewCluster(MemberCount(), xmlConfig(DefaultClusterName, port), port)
})
var idGen = proxy.ReferenceIDGenerator{}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func Tester(t *testing.T, f func(t *testing.T, client *hz.Client)) {
	TesterWithConfigBuilder(t, nil, f)
}

func TesterWithConfigBuilder(t *testing.T, cbCallback func(config *hz.Config), f func(t *testing.T, client *hz.Client)) {
	ensureRemoteController(true)
	runner := func(t *testing.T, smart bool) {
		if LeakCheckEnabled() {
			t.Logf("enabled leak check")
			defer goleak.VerifyNone(t)
		}
		cls := defaultTestCluster.Launch(t)
		config := cls.DefaultConfig()
		if cbCallback != nil {
			cbCallback(&config)
		}
		logLevel := logger.WarnLevel
		if TraceLoggingEnabled() {
			logLevel = logger.TraceLevel
		}
		config.Logger.Level = logLevel
		config.Cluster.Unisocket = !smart
		ctx := context.Background()
		client := MustClient(hz.StartNewClientWithConfig(ctx, config))
		defer func() {
			if err := client.Shutdown(ctx); err != nil {
				t.Logf("Test warning, client did not shut down: %s", err.Error())
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

func (s SamplePortable) WritePortable(writer serialization.PortableWriter) {
	writer.WriteString("A", s.A)
	writer.WriteInt32("B", s.B)
}

func (s *SamplePortable) ReadPortable(reader serialization.PortableReader) {
	s.A = reader.ReadString("A")
	s.B = reader.ReadInt32("B")
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

// Must panics if err is not nil.
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

// MustSlice returns a slice of values if err is nil, otherwise it panics.
func MustSlice(slice []interface{}, err error) []interface{} {
	if err != nil {
		panic(err)
	}
	return slice
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

func NewUniqueObjectName(service string, labels ...string) string {
	ls := strings.Join(labels, "_")
	if ls != "" {
		ls = fmt.Sprintf("-%s", ls)
	}
	return fmt.Sprintf("test-%s-%d-%d%s", service, idGen.NextID(), rand.Int(), ls)
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

func SSLEnabled() bool {
	return os.Getenv(EnvEnableSSL) == "1"
}

func HzVersion() string {
	version := os.Getenv(EnvHzVersion)
	if version == "" {
		version = "5.1"
	}
	return version
}

func MemberCount() int {
	if memberCountStr := os.Getenv(EnvMemberCount); memberCountStr != "" {
		if memberCount, err := strconv.Atoi(memberCountStr); err != nil {
			panic(err)
		} else {
			return memberCount
		}
	}
	return 1
}

func CreateDefaultRemoteController() *RemoteControllerClientWrapper {
	return newRemoteControllerClientWrapper(CreateRemoteController("localhost:9701"))
}

func CreateRemoteController(addr string) *RemoteControllerClient {
	transport := MustValue(thrift.NewTSocketConf(addr, nil)).(*thrift.TSocket)
	bufferedTransport := thrift.NewTBufferedTransport(transport, 4096)
	protocol := thrift.NewTBinaryProtocolConf(bufferedTransport, nil)
	client := thrift.NewTStandardClient(protocol, protocol)
	rc := NewRemoteControllerClient(client)
	Must(transport.Open())
	return rc
}

func ensureRemoteController(launchDefaultCluster bool) *RemoteControllerClientWrapper {
	rcMu.Lock()
	defer rcMu.Unlock()
	if rc == nil {
		rc = CreateDefaultRemoteController()
		if ping, err := rc.Ping(context.Background()); err != nil {
			panic(err)
		} else if !ping {
			panic("remote controller not accesible")
		}
	}
	return rc
}

func StartNewClusterWithOptions(clusterName string, port, memberCount int) *TestCluster {
	ensureRemoteController(false)
	config := xmlConfig(clusterName, port)
	if SSLEnabled() {
		config = xmlSSLConfig(clusterName, port)
	}
	return rc.startNewCluster(memberCount, config, port)
}

func StartNewClusterWithConfig(memberCount int, config string, port int) *TestCluster {
	ensureRemoteController(false)
	return rc.startNewCluster(memberCount, config, port)
}

type RemoteControllerClientWrapper struct {
	mu *sync.Mutex
	rc *RemoteControllerClient
}

func newRemoteControllerClientWrapper(rc *RemoteControllerClient) *RemoteControllerClientWrapper {
	return &RemoteControllerClientWrapper{
		mu: &sync.Mutex{},
		rc: rc,
	}
}

func (rcw *RemoteControllerClientWrapper) startNewCluster(memberCount int, config string, port int) *TestCluster {
	cluster := MustValue(rcw.CreateClusterKeepClusterName(context.Background(), HzVersion(), config)).(*Cluster)
	memberUUIDs := make([]string, 0, memberCount)
	for i := 0; i < memberCount; i++ {
		member := MustValue(rcw.StartMember(context.Background(), cluster.ID)).(*Member)
		memberUUIDs = append(memberUUIDs, member.UUID)
	}
	return &TestCluster{
		RC:          rcw,
		ClusterID:   cluster.ID,
		MemberUUIDs: memberUUIDs,
		Port:        port,
	}
}

func (rcw *RemoteControllerClientWrapper) StartMember(ctx context.Context, clusterID string) (*Member, error) {
	rcw.mu.Lock()
	defer rcw.mu.Unlock()
	return rcw.rc.StartMember(ctx, clusterID)
}

func (rcw *RemoteControllerClientWrapper) Ping(ctx context.Context) (bool, error) {
	rcw.mu.Lock()
	defer rcw.mu.Unlock()
	return rcw.rc.Ping(ctx)
}

func (rcw *RemoteControllerClientWrapper) CreateClusterKeepClusterName(ctx context.Context, hzVersion string, xmlconfig string) (*Cluster, error) {
	rcw.mu.Lock()
	defer rcw.mu.Unlock()
	return rcw.rc.CreateClusterKeepClusterName(ctx, hzVersion, xmlconfig)
}

func (rcw *RemoteControllerClientWrapper) ShutdownMember(ctx context.Context, clusterID string, memberID string) (bool, error) {
	rcw.mu.Lock()
	defer rcw.mu.Unlock()
	return rcw.rc.ShutdownMember(ctx, clusterID, memberID)
}

func (rcw *RemoteControllerClientWrapper) TerminateMember(ctx context.Context, clusterID string, memberID string) (bool, error) {
	rcw.mu.Lock()
	defer rcw.mu.Unlock()
	return rcw.rc.TerminateMember(ctx, clusterID, memberID)
}

func (rcw *RemoteControllerClientWrapper) ExecuteOnController(ctx context.Context, clusterID string, script string, lang Lang) (*Response, error) {
	rcw.mu.Lock()
	defer rcw.mu.Unlock()
	return rcw.rc.ExecuteOnController(ctx, clusterID, script, lang)
}

type TestCluster struct {
	RC          *RemoteControllerClientWrapper
	ClusterID   string
	MemberUUIDs []string
	Port        int
}

func (c TestCluster) Shutdown() {
	// TODO: add Terminate method.
	for _, memberUUID := range c.MemberUUIDs {
		c.RC.ShutdownMember(context.Background(), c.ClusterID, memberUUID)
	}
}

func (c TestCluster) DefaultConfig() hz.Config {
	config := hz.Config{}
	config.Cluster.Name = c.ClusterID
	config.Cluster.Network.SetAddresses(fmt.Sprintf("localhost:%d", c.Port))
	if SSLEnabled() {
		config.Cluster.Network.SSL.Enabled = true
		config.Cluster.Network.SSL.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}
	if TraceLoggingEnabled() {
		config.Logger.Level = logger.TraceLevel
	}
	return config
}

func (c TestCluster) DefaultConfigWithNoSSL() hz.Config {
	config := hz.Config{}
	config.Cluster.Name = c.ClusterID
	config.Cluster.Network.SetAddresses(fmt.Sprintf("localhost:%d", c.Port))
	if TraceLoggingEnabled() {
		config.Logger.Level = logger.TraceLevel
	}
	return config
}

func xmlConfig(clusterName string, port int) string {
	return fmt.Sprintf(`
        <hazelcast xmlns="http://www.hazelcast.com/schema/config"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.hazelcast.com/schema/config
            http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
            <cluster-name>%s</cluster-name>
            <network>
               <port>%d</port>
            </network>
			<map name="test-map">
				<map-store enabled="true">
					<class-name>com.hazelcast.client.test.SampleMapStore</class-name>
				</map-store>
			</map>
			<map name="test-map-smart">
				<map-store enabled="true">
					<class-name>com.hazelcast.client.test.SampleMapStore</class-name>
				</map-store>
			</map>
			<map name="test-map-unisocket">
				<map-store enabled="true">
					<class-name>com.hazelcast.client.test.SampleMapStore</class-name>
				</map-store>
			</map>
			<serialization>
				<data-serializable-factories>
					<data-serializable-factory factory-id="66">com.hazelcast.client.test.IdentifiedFactory</data-serializable-factory>
					<data-serializable-factory factory-id="666">com.hazelcast.client.test.IdentifiedDataSerializableFactory</data-serializable-factory>
				</data-serializable-factories>
			</serialization>
        </hazelcast>
	`, clusterName, port)
}

func xmlSSLConfig(clusterName string, port int) string {
	return fmt.Sprintf(`
		<hazelcast xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://www.hazelcast.com/schema/config
           http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
			<cluster-name>%s</cluster-name>
			<network>
			   <port>%d</port>
				<ssl enabled="true">
					<factory-class-name>
						com.hazelcast.nio.ssl.ClasspathSSLContextFactory
					</factory-class-name>
					<properties>
						<property name="keyStore">com/hazelcast/nio/ssl-mutual-auth/server1.keystore</property>
						<property name="keyStorePassword">password.txt</property>
						<property name="keyManagerAlgorithm">SunX509</property>
						<property name="protocol">TLSv1.2</property>
					</properties>
				</ssl>
			</network>
			<map name="test-map">
				<map-store enabled="true">
					<class-name>com.hazelcast.client.test.SampleMapStore</class-name>
				</map-store>
			</map>
			<serialization>
				<data-serializable-factories>
					<data-serializable-factory factory-id="66">com.hazelcast.client.test.IdentifiedFactory</data-serializable-factory>
					<data-serializable-factory factory-id="666">com.hazelcast.client.test.IdentifiedDataSerializableFactory</data-serializable-factory>
				</data-serializable-factories>
			</serialization>
		</hazelcast>
			`, clusterName, port)
}

func getLoggerLevel() logger.Level {
	if TraceLoggingEnabled() {
		return logger.TraceLevel
	}
	return logger.InfoLevel
}

func getDefaultClient(config *hz.Config) *hz.Client {
	lv := getLoggerLevel()
	if lv == logger.TraceLevel {
		config.Logger.Level = lv
	}
	client, err := hz.StartNewClientWithConfig(context.Background(), *config)
	if err != nil {
		panic(err)
	}
	return client
}

// Eventually asserts that given condition will be met in 2 minutes,
// checking target function every 200 milliseconds.
func Eventually(t *testing.T, condition func() bool, msgAndArgs ...interface{}) {
	if !assert.Eventually(t, condition, time.Minute*2, time.Millisecond*200, msgAndArgs...) {
		t.FailNow()
	}
}

// Never asserts that the given condition doesn't satisfy in 3 seconds,
// checking target function every 200 milliseconds.
//
func Never(t *testing.T, condition func() bool, msgAndArgs ...interface{}) {
	if !assert.Never(t, condition, time.Second*3, time.Millisecond*200, msgAndArgs) {
		t.FailNow()
	}
}

// WaitEventually waits for the waitgroup for 2 minutes
// Fails the test if 2 mimutes is reached.
func WaitEventually(t *testing.T, wg *sync.WaitGroup) {
	WaitEventuallyWithTimeout(t, wg, time.Minute*2)
}

// WaitEventuallyWithTimeout waits for the waitgroup for the specified max timeout.
// Fails the test if given timeout is reached.
func WaitEventuallyWithTimeout(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-c:
		//done successfully
	case <-timer.C:
		t.FailNow()
	}
}

func EqualStringContent(b1, b2 []byte) bool {
	s1 := sortedString(b1)
	s2 := sortedString(b2)
	return s1 == s2
}

func sortedString(b []byte) string {
	bc := make([]byte, len(b))
	copy(bc, b)
	sort.Slice(bc, func(i, j int) bool {
		return bc[i] < bc[j]
	})
	s := strings.ReplaceAll(string(bc), " ", "")
	s = strings.ReplaceAll(s, "\t", "")
	s = strings.ReplaceAll(s, "\n", "")
	s = strings.ReplaceAll(s, "\r", "")
	return s
}

var nextPort int32 = 10000

func NextPort() int {
	// let minimum step be 10, just a round, safe value.
	// note that some tests may not use the MemberCount() function for the cluster size.
	const maxStep = 10
	step := MemberCount()
	if step < maxStep {
		step = maxStep
	}
nextblock:
	for {
		start := int(atomic.AddInt32(&nextPort, int32(step))) - step
		// check that all ports in the range are open
		for port := start; port < start+step; port++ {
			if !isPortOpen(port) {
				// ignoring the error from fmt.Fprintf, not useful in this case.
				_, _ = fmt.Fprintf(os.Stderr, "it.NextPort: %d is not open, skipping the block: [%d:%d]\n", port, start, start+step)
				continue nextblock
			}
		}
		return start
	}
}

func isPortOpen(port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", port), 10*time.Millisecond)
	if err != nil {
		return true
	}
	// ignoring the error from conn.Close, since there's nothing useful to do with it.
	_ = conn.Close()
	return false
}

type SingletonTestCluster struct {
	mu       *sync.Mutex
	cls      *TestCluster
	launcher func() *TestCluster
	name     string
}

func NewSingletonTestCluster(name string, launcher func() *TestCluster) *SingletonTestCluster {
	return &SingletonTestCluster{
		name:     name,
		mu:       &sync.Mutex{},
		launcher: launcher,
	}
}

type testLogger interface {
	Logf(format string, args ...interface{})
}

func (c *SingletonTestCluster) Launch(t testLogger) *TestCluster {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cls != nil {
		return c.cls
	}
	t.Logf("Launching the auto-shutdown test cluster: %s", c.name)
	c.cls = c.launcher()
	return c.cls
}
