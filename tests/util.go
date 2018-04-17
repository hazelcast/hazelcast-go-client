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
	"io/ioutil"
	"sync"
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

func Read(filename string) (string, error) {
	bytes, err := ioutil.ReadFile(filename)
	return string(bytes), err
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
