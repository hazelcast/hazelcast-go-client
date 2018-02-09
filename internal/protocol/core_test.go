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

package protocol

import (
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"testing"
)

func TestMember_Equal(t *testing.T) {
	member1 := NewMember(*NewAddress(), "test", false, nil)
	member2 := NewMember(*NewAddress(), "test1", false, nil)

	if ok := member1.Equal(*member2); ok {
		t.Fatal("memberEqual test failed")
	}
	member2.isLiteMember = true
	member2.uuid = "test"

	if ok := member1.Equal(*member2); ok {
		t.Fatal("memberEqual test failed")
	}

	member2.isLiteMember = false
	testMap := make(map[string]string)
	testMap["test"] = "test"
	member2.attributes = testMap

	if ok := member1.Equal(*member2); ok {
		t.Fatal("memberEqual test failed")
	}

}

func TestDistributedObjectInfo_ServiceName(t *testing.T) {
	obj := DistributedObjectInfo{"testName", "testServiceName"}
	if result := obj.ServiceName(); result != "testServiceName" {
		t.Fatal("distributed object info serviceName failed")
	}
}

func TestDistributedObjectInfo_Name(t *testing.T) {
	obj := DistributedObjectInfo{"testName", "testServiceName"}
	if result := obj.Name(); result != "testName" {
		t.Fatal("distributed object info serviceName failed")
	}
}

func TestDataEntryView_Equal(t *testing.T) {
	dataEntryView := DataEntryView{}
	payload1 := make([]byte, 5)
	payload1[0] = 1
	dataEntryView.keyData = serialization.NewData(payload1)
	dataEntryView.valueData = serialization.NewData(payload1)
	dataEntryView2 := DataEntryView{}
	payload2 := make([]byte, 5)
	payload2[0] = 2
	dataEntryView2.keyData = serialization.NewData(payload2)
	dataEntryView2.valueData = serialization.NewData(payload1)
	if ok := dataEntryView.Equal(dataEntryView2); ok {
		t.Fatal("Data EntryView Equal failed")
	}

	dataEntryView2.keyData = serialization.NewData(payload1)
	dataEntryView2.cost = 5

	if ok := dataEntryView.Equal(dataEntryView2); ok {
		t.Fatal("Data EntryView Equal failed")
	}

	dataEntryView.cost = 5
	dataEntryView.lastAccessTime = 5

	if ok := dataEntryView.Equal(dataEntryView2); ok {
		t.Fatal("Data EntryView Equal failed")
	}

	dataEntryView2.lastAccessTime = 5
	dataEntryView2.version = 5

	if ok := dataEntryView.Equal(dataEntryView2); ok {
		t.Fatal("Data EntryView Equal failed")
	}

}
