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

package ringbuffer

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/rc"
	. "github.com/hazelcast/hazelcast-go-client/tests"
	"log"
	"testing"
)

var ringbuffer core.Ringbuffer
var client hazelcast.IHazelcastInstance

const capacity int64 = 10
const ringbufferName = "ClientRingbufferTestWithTTL"

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()
	ringbuffer, _ = client.GetRingbuffer(ringbufferName)
	m.Run()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestRingbufferProxy_Name(t *testing.T) {
	if ringbufferName != ringbuffer.Name() {
		t.Errorf("Name() failed")
	}
}

func TestRingbufferProxy_ServiceName(t *testing.T) {
	serviceName := common.ServiceNameRingbufferService
	if serviceName != ringbuffer.ServiceName() {
		t.Errorf("ServiceName() failed")
	}
}

func TestRingbufferProxy_PartitionKey(t *testing.T) {
	if ringbufferName != ringbuffer.PartitionKey() {
		t.Errorf("PartitionKey() failed")
	}
}

func fillRingbuffer(capacity int64) {
	for i := int64(0); i < capacity; i++ {
		ringbuffer.Add(i, core.OverflowPolicyOverwrite)
	}
}
func destroyAndCreate() {
	ringbuffer.Destroy()
	ringbuffer, _ = client.GetRingbuffer(ringbufferName)
}

func TestRingbufferProxy_Capacity(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	cap, err := ringbuffer.Capacity()
	AssertEqualf(t, err, cap, capacity, "ringbuffer Capacity() failed")
}

func TestRingbufferProxy_Add(t *testing.T) {
	defer destroyAndCreate()
	sequence, _ := ringbuffer.Add("value", core.OverflowPolicyOverwrite)
	item, err := ringbuffer.ReadOne(sequence)
	AssertEqualf(t, err, "value", item, "ringbuffer Add() failed")
}

func TestRingbufferProxy_Size(t *testing.T) {
	defer destroyAndCreate()
	items := make([]interface{}, capacity)
	for i := int64(0); i < capacity; i++ {
		items[i] = i
	}
	_, err := ringbuffer.AddAll(items, core.OverflowPolicyFail)
	size, err := ringbuffer.Size()
	AssertEqualf(t, err, size, capacity, "ringbuffer Size-w() failed")
}

func TestRingbufferProxy_TailSequence(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity * 2)
	headSequence, err := ringbuffer.HeadSequence()
	AssertEqualf(t, err, capacity, headSequence, "ringbuffer HeadSequence() failed")
}

func TestRingbufferProxy_HeadSequence(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity * 2)
	tailSequence, err := ringbuffer.TailSequence()
	AssertEqualf(t, err, capacity*2-1, tailSequence, "ringbuffer TailSequence() failed")

}

func TestRingbufferProxy_RemainingCapacity(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity / 2)
	remainingCapacity, err := ringbuffer.RemainingCapacity()
	AssertEqualf(t, err, capacity/2, remainingCapacity, "ringbuffer RemainingCapacity() failed")

}

func TestRingbufferProxy_AddWhenFull(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	sequence, err := ringbuffer.Add(capacity+1, core.OverflowPolicyFail)
	AssertEqualf(t, err, int64(-1), sequence, "ringbuffer Add() failed")

}

func TestRingbufferProxy_AddAll(t *testing.T) {
	defer destroyAndCreate()
	items := make([]interface{}, capacity)
	for i := int64(0); i < capacity; i++ {
		items[i] = i
	}
	lastSequence, err := ringbuffer.AddAll(items, core.OverflowPolicyFail)
	AssertEqualf(t, err, capacity-1, lastSequence, "ringbuffer AddAll() failed")
}

func TestRingbufferProxy_AddAllWhenFull(t *testing.T) {
	defer destroyAndCreate()
	items := make([]interface{}, capacity*2)
	for i := int64(0); i < capacity*2; i++ {
		items[i] = i
	}
	lastSequence, err := ringbuffer.AddAll(items, core.OverflowPolicyFail)
	AssertEqualf(t, err, int64(-1), lastSequence, "ringbuffer AddAll() failed")
}

func TestRingbufferProxy_RemainingCapacity2(t *testing.T) {
	defer destroyAndCreate()
	_, err := ringbuffer.Add("value", core.OverflowPolicyOverwrite)
	remainingCapacity, err := ringbuffer.RemainingCapacity()
	AssertEqualf(t, err, capacity-1, remainingCapacity, "ringbuffer RemainingCapacity() failed")
}

func TestRingbufferProxy_ReadOne(t *testing.T) {
	defer destroyAndCreate()
	ringbuffer.Add("item", core.OverflowPolicyOverwrite)
	ringbuffer.Add("item-2", core.OverflowPolicyOverwrite)
	ringbuffer.Add("item-3", core.OverflowPolicyOverwrite)
	item, err := ringbuffer.ReadOne(0)
	AssertEqualf(t, err, "item", item, "ringbuffer ReadOne() failed")
	item, err = ringbuffer.ReadOne(1)
	AssertEqualf(t, err, "item-2", item, "ringbuffer ReadOne() failed")
	item, err = ringbuffer.ReadOne(2)
	AssertEqualf(t, err, "item-3", item, "ringbuffer ReadOne() failed")
}

func TestRingbufferProxy_ReadMany(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	items := make([]interface{}, capacity)
	for i := int64(0); i < capacity; i++ {
		items[i] = i
	}
	readResultSet, err := ringbuffer.ReadMany(0, 0, int32(capacity), nil)
	AssertEqualf(t, err, int64(readResultSet.Size()), capacity, "ringbuffer ReadMany() failed")
	for i := int32(0); i < readResultSet.Size(); i++ {
		read, err := readResultSet.Get(i)
		AssertEqualf(t, err, read.(int64), int64(i), "ringbuffer ReadMany() failed")
	}
}

func TestRingbufferProxy_ReadMany_MinLargerThanMax(t *testing.T) {
	_, err := ringbuffer.ReadMany(0, 2, 0, nil)
	AssertErrorNotNil(t, err, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_MinNegative(t *testing.T) {
	_, err := ringbuffer.ReadMany(0, -1, 3, nil)
	AssertErrorNotNil(t, err, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResulSetWithFilter(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	var expectedValue int64 = 2
	resultSet, err := ringbuffer.ReadMany(0, 3, 3, nil)
	retValue, err := resultSet.Get(2)
	AssertEqualf(t, err, retValue.(int64), expectedValue, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResulSet_Get(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	var expectedValue int64 = 2
	resultSet, err := ringbuffer.ReadMany(0, 3, 3, nil)
	retValue, err := resultSet.Get(2)
	AssertEqualf(t, err, retValue.(int64), expectedValue, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResulSet_GetWithIllegalIndex(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	resultSet, err := ringbuffer.ReadMany(0, 3, 3, nil)
	_, err = resultSet.Get(4)
	AssertErrorNotNil(t, err, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResulSet_Sequence(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	var expectedValue int64 = 4
	resultSet, err := ringbuffer.ReadMany(0, 5, 5, nil)
	retValue, err := resultSet.Sequence(4)
	AssertEqualf(t, err, retValue, expectedValue, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResulSet_SequenceWithIllegalIndex(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	resultSet, err := ringbuffer.ReadMany(0, 3, 3, nil)
	_, err = resultSet.Sequence(4)
	AssertErrorNotNil(t, err, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResulSet_Size(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	var expectedValue int32 = 5
	resultSet, err := ringbuffer.ReadMany(0, 5, 5, nil)
	retValue := resultSet.Size()
	AssertEqualf(t, err, retValue, expectedValue, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResulSet_ReadCount(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	var expectedValue int32 = 5
	resultSet, err := ringbuffer.ReadMany(0, 5, 5, nil)
	retValue := resultSet.ReadCount()
	AssertEqualf(t, err, retValue, expectedValue, "ringbuffer ReadMany() failed")
}
