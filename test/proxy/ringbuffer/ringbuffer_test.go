// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
	"log"
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ringbuffer core.Ringbuffer
var client hazelcast.Client

const capacity int64 = 10
const ringbufferName = "ClientRingbufferTestWithTTL"

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewClient()
	ringbuffer, _ = client.GetRingbuffer(ringbufferName)
	m.Run()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestRingbufferProxy_Name(t *testing.T) {
	if ringbufferName != ringbuffer.Name() {
		t.Error("Name() failed")
	}
}

func TestRingbufferProxy_ServiceName(t *testing.T) {
	serviceName := bufutil.ServiceNameRingbufferService
	if serviceName != ringbuffer.ServiceName() {
		t.Error("ServiceName() failed")
	}
}

func TestRingbufferProxy_PartitionKey(t *testing.T) {
	if ringbufferName != ringbuffer.PartitionKey() {
		t.Error("PartitionKey() failed")
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
	require.NoError(t, err)
	assert.Equalf(t, cap, capacity, "ringbuffer Capacity() failed")
}

func TestRingbufferProxy_Add(t *testing.T) {
	defer destroyAndCreate()
	sequence, _ := ringbuffer.Add("value", core.OverflowPolicyOverwrite)
	item, err := ringbuffer.ReadOne(sequence)
	require.NoError(t, err)
	assert.Equalf(t, "value", item, "ringbuffer Add() failed")
}

func TestRingbufferProxy_AddNonSerializable(t *testing.T) {
	defer destroyAndCreate()
	_, err := ringbuffer.Add(testutil.NewNonSerializableObject(), core.OverflowPolicyOverwrite)
	require.Error(t, err)
}

func TestRingbufferProxy_Size(t *testing.T) {
	defer destroyAndCreate()
	items := make([]interface{}, capacity)
	for i := int64(0); i < capacity; i++ {
		items[i] = i
	}
	_, err := ringbuffer.AddAll(items, core.OverflowPolicyFail)
	require.NoError(t, err)
	size, err := ringbuffer.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, capacity, "ringbuffer Size-w() failed")
}

func TestRingbufferProxy_TailSequence(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity * 2)
	headSequence, err := ringbuffer.HeadSequence()
	require.NoError(t, err)
	assert.Equalf(t, capacity, headSequence, "ringbuffer HeadSequence() failed")
}

func TestRingbufferProxy_HeadSequence(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity * 2)
	tailSequence, err := ringbuffer.TailSequence()
	require.NoError(t, err)
	assert.Equalf(t, capacity*2-1, tailSequence, "ringbuffer TailSequence() failed")

}

func TestRingbufferProxy_RemainingCapacity(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity / 2)
	remainingCapacity, err := ringbuffer.RemainingCapacity()
	require.NoError(t, err)
	assert.Equalf(t, capacity/2, remainingCapacity, "ringbuffer RemainingCapacity() failed")

}

func TestRingbufferProxy_AddWhenFull(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	sequence, err := ringbuffer.Add(capacity+1, core.OverflowPolicyFail)
	require.NoError(t, err)
	assert.Equalf(t, int64(-1), sequence, "ringbuffer Add() failed")

}

func TestRingbufferProxy_AddAll(t *testing.T) {
	defer destroyAndCreate()
	items := make([]interface{}, capacity)
	for i := int64(0); i < capacity; i++ {
		items[i] = i
	}
	lastSequence, err := ringbuffer.AddAll(items, core.OverflowPolicyFail)
	require.NoError(t, err)
	assert.Equalf(t, capacity-1, lastSequence, "ringbuffer AddAll() failed")
}

func TestRingbufferProxy_AddAllNonSerializable(t *testing.T) {
	defer destroyAndCreate()
	_, err := ringbuffer.AddAll(testutil.NewNonSerializableObjectSlice(), core.OverflowPolicyFail)
	require.Error(t, err)
}

func TestRingbufferProxy_AddAllWhenFull(t *testing.T) {
	defer destroyAndCreate()
	items := make([]interface{}, capacity*2)
	for i := int64(0); i < capacity*2; i++ {
		items[i] = i
	}
	lastSequence, err := ringbuffer.AddAll(items, core.OverflowPolicyFail)
	require.NoError(t, err)
	assert.Equalf(t, int64(-1), lastSequence, "ringbuffer AddAll() failed")
}

func TestRingbufferProxy_RemainingCapacity2(t *testing.T) {
	defer destroyAndCreate()
	_, err := ringbuffer.Add("value", core.OverflowPolicyOverwrite)
	require.NoError(t, err)
	remainingCapacity, err := ringbuffer.RemainingCapacity()
	require.NoError(t, err)
	assert.Equalf(t, capacity-1, remainingCapacity, "ringbuffer RemainingCapacity() failed")
}

func TestRingbufferProxy_ReadOne(t *testing.T) {
	defer destroyAndCreate()
	ringbuffer.Add("item", core.OverflowPolicyOverwrite)
	ringbuffer.Add("item-2", core.OverflowPolicyOverwrite)
	ringbuffer.Add("item-3", core.OverflowPolicyOverwrite)
	item, err := ringbuffer.ReadOne(0)
	require.NoError(t, err)
	assert.Equalf(t, "item", item, "ringbuffer ReadOne() failed")
	item, err = ringbuffer.ReadOne(1)
	require.NoError(t, err)
	assert.Equalf(t, "item-2", item, "ringbuffer ReadOne() failed")
	item, err = ringbuffer.ReadOne(2)
	require.NoError(t, err)
	assert.Equalf(t, "item-3", item, "ringbuffer ReadOne() failed")
}

func TestRingbufferProxy_ReadMany(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	items := make([]interface{}, capacity)
	for i := int64(0); i < capacity; i++ {
		items[i] = i
	}
	readResultSet, err := ringbuffer.ReadMany(0, 0, int32(capacity), nil)
	require.NoError(t, err)
	assert.Equalf(t, int64(readResultSet.Size()), capacity, "ringbuffer ReadMany() failed")
	for i := int32(0); i < readResultSet.Size(); i++ {
		read, err := readResultSet.Get(i)
		require.NoError(t, err)
		assert.Equalf(t, read.(int64), int64(i), "ringbuffer ReadMany() failed")
	}
}

func TestRingbufferProxy_ReadMany_NonSerializableFilter(t *testing.T) {
	defer destroyAndCreate()
	_, err := ringbuffer.ReadMany(0, 0, 0, testutil.NewNonSerializableObject())
	assert.Error(t, err)
}

func TestRingbufferProxy_ReadOne_NegativeSequence(t *testing.T) {
	defer destroyAndCreate()
	_, err := ringbuffer.ReadOne(-1)
	assert.Error(t, err)
}

func TestRingbufferProxy_ReadMany_NegativeSequence(t *testing.T) {
	defer destroyAndCreate()
	_, err := ringbuffer.ReadMany(-1, 0, 0, nil)
	assert.Error(t, err)
}

func TestRingbufferProxy_ReadMany_MinLargerThanMax(t *testing.T) {
	_, err := ringbuffer.ReadMany(0, 2, 0, nil)
	require.Errorf(t, err, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_MinNegative(t *testing.T) {
	_, err := ringbuffer.ReadMany(0, -1, 3, nil)
	require.Errorf(t, err, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResultSetWithFilter(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	var expectedValue int64 = 2
	resultSet, err := ringbuffer.ReadMany(0, 3, 3, nil)
	require.NoError(t, err)
	retValue, err := resultSet.Get(2)
	require.NoError(t, err)
	assert.Equalf(t, retValue.(int64), expectedValue, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResultSet_Get(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	var expectedValue int64 = 2
	resultSet, err := ringbuffer.ReadMany(0, 3, 3, nil)
	require.NoError(t, err)
	retValue, err := resultSet.Get(2)
	require.NoError(t, err)
	assert.Equalf(t, retValue.(int64), expectedValue, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResultSet_GetWithIllegalIndex(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	resultSet, err := ringbuffer.ReadMany(0, 3, 3, nil)
	require.NoError(t, err)
	_, err = resultSet.Get(4)
	require.Errorf(t, err, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResultSet_Sequence(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	var expectedValue int64 = 4
	resultSet, err := ringbuffer.ReadMany(0, 5, 5, nil)
	require.NoError(t, err)
	retValue, err := resultSet.Sequence(4)
	require.NoError(t, err)
	assert.Equalf(t, retValue, expectedValue, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResultSet_SequenceWithIllegalIndex(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	resultSet, err := ringbuffer.ReadMany(0, 3, 3, nil)
	require.NoError(t, err)
	_, err = resultSet.Sequence(4)
	require.Errorf(t, err, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResultSet_Size(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	var expectedValue int32 = 5
	resultSet, err := ringbuffer.ReadMany(0, 5, 5, nil)
	retValue := resultSet.Size()
	require.NoError(t, err)
	assert.Equalf(t, retValue, expectedValue, "ringbuffer ReadMany() failed")
}

func TestRingbufferProxy_ReadMany_ResultSet_ReadCount(t *testing.T) {
	defer destroyAndCreate()
	fillRingbuffer(capacity)
	var expectedValue int32 = 5
	resultSet, err := ringbuffer.ReadMany(0, 5, 5, nil)
	retValue := resultSet.ReadCount()
	require.NoError(t, err)
	assert.Equalf(t, retValue, expectedValue, "ringbuffer ReadMany() failed")
}
