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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
)

func TestRemoveAddr(t *testing.T) {
	cm := newConnectionMap(pubcluster.NewRoundRobinLoadBalancer())
	conn1 := &Connection{connectionID: 1}
	conn2 := &Connection{connectionID: 2}
	conn3 := &Connection{connectionID: 3}
	cm.removeAddr("1.2.3.4:5600")
	assert.Equal(t, 0, len(cm.addrs))
	cm.AddConnection(conn1, "100.200.300.400:5678")
	cm.AddConnection(conn2, "100.200.300.401:5678")
	cm.AddConnection(conn3, "100.200.300.402:5678")
	if !assert.Equal(t, []pubcluster.Address{"100.200.300.400:5678", "100.200.300.401:5678", "100.200.300.402:5678"}, cm.addrs) {
		t.FailNow()
	}
	cm.removeAddr("100.200.300.401:5678")
	if !assert.Equal(t, []pubcluster.Address{"100.200.300.400:5678", "100.200.300.402:5678"}, cm.addrs) {
		t.FailNow()
	}
	cm.removeAddr("100.200.300.402:5678")
	if !assert.Equal(t, []pubcluster.Address{"100.200.300.400:5678"}, cm.addrs) {
		t.FailNow()
	}
	cm.removeAddr("100.200.300.400:5678")
	if !assert.Equal(t, []pubcluster.Address{}, cm.addrs) {
		t.FailNow()
	}
}
