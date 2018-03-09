// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	"reflect"
	"sync"
	"testing"
	"time"
)

var Timeout time.Duration = 1 * time.Minute

const DefaultServerConfig = `
<hazelcast xsi:schemaLocation="http://www.hazelcast.com/schema/config hazelcast-config-3.9.xsd"
           xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <group>
        <name>dev</name>
        <password>dev-pass</password>
    </group>
    <management-center enabled="false">http://localhost:8080/mancenter</management-center>
    <network>
        <port auto-increment="true" port-count="100">5701</port>
        <outbound-ports>
            <!--
            Allowed port range when connecting to other nodes.
            0 or * means use system provided port.
            -->
            <ports>0</ports>
        </outbound-ports>
        <join>
            <multicast enabled="true">
                <multicast-group>224.7.7.7</multicast-group>
                <multicast-port>54327</multicast-port>
            </multicast>
            <tcp-ip enabled="false">
                <interface>127.0.0.1</interface>
            </tcp-ip>
        </join>
        <public-address>127.0.0.1</public-address>
        <ssl enabled="false"/>
        <socket-interceptor enabled="false"/>
    </network>
    <serialization>
        <data-serializable-factories>
            <data-serializable-factory factory-id="66">com.hazelcast.client.test.IdentifiedFactory
            </data-serializable-factory>
        </data-serializable-factories>
    </serialization>

    <queue name="ClientQueueTest*">
        <!--
            Maximum size of the queue. When a JVM's local queue size reaches the maximum,
            all put/offer operations will get blocked until the queue size
            of the JVM goes down below the maximum.
            Any integer between 0 and Integer.MAX_VALUE. 0 means
            Integer.MAX_VALUE. Default is 0.
        -->
        <max-size>6</max-size>
    </queue>
    <ringbuffer name="ClientRingbufferTest*">
        <capacity>10</capacity>
    </ringbuffer>
    <ringbuffer name="ClientRingbufferTestWithTTL*">
        <capacity>10</capacity>
        <time-to-live-seconds>180</time-to-live-seconds>
    </ringbuffer>
</hazelcast>
`

func AssertEqualf(t *testing.T, err error, l interface{}, r interface{}, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(l, r) {
		t.Fatalf("%v != %v : %v", l, r, message)
	}
}
func AssertNilf(t *testing.T, err error, l interface{}, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if l != nil {
		t.Fatalf("%v != nil : %v", l, message)
	}
}
func AssertErrorNotNil(t *testing.T, err error, message string) {
	if err == nil {
		t.Fatal(message)
	}
}

func AssertErrorNil(t *testing.T, err error) {
	if err != nil {
		t.Error(err.Error())
	}
}

func AssertMapEqualPairSlice(t *testing.T, err error, mp map[interface{}]interface{}, pairSlice []core.IPair, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if len(mp) != len(pairSlice) {
		t.Fatal(message)
	}
	for _, pair := range pairSlice {
		key := pair.Key()
		value := pair.Value()
		expectedValue, found := mp[key]
		if !found || expectedValue != value {
			t.Fatal(message)
		}
	}
}

func AssertSlicesHaveSameElements(t *testing.T, err error, arg1 []interface{}, arg2 []interface{}, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if len(arg1) != len(arg2) {
		t.Fatal(message)
	}
	for _, elem := range arg1 {
		found := false
		for _, elem2 := range arg2 {
			if elem == elem2 {
				found = true
			}
		}
		if !found {
			t.Fatal(message)
		}
	}

}

func AssertEqual(t *testing.T, err error, l interface{}, r interface{}) {
	if err != nil {
		t.Fatal(err)
	}
	if l != r {
		t.Fatalf("%v != %v", l, r)
	}

}
func WaitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
