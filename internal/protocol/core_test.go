// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/go-client/internal/serialization"
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

func TestDataEntryView_Cost(t *testing.T) {
	entryView := EntryView{}
	entryView.cost = 5
	if result := entryView.Cost(); result != 5 {
		t.Fatal("entryView Cost failed")
	}
}

func TestDataEntryView_CreationTime(t *testing.T) {
	entryView := EntryView{}
	entryView.creationTime = 5
	if result := entryView.CreationTime(); result != 5 {
		t.Fatal("entryView CreationTime failed")
	}
}

func TestDataEntryView_ExpirationTime(t *testing.T) {
	entryView := EntryView{}
	entryView.expirationTime = 5
	if result := entryView.ExpirationTime(); result != 5 {
		t.Fatal("entryView ExpirationTime failed")
	}
}

func TestDataEntryView_LastAccessTimeTime(t *testing.T) {
	entryView := EntryView{}
	entryView.lastAccessTime = 5
	if result := entryView.LastAccessTime(); result != 5 {
		t.Fatal("entryView LastAccessTime failed")
	}
}

func TestDataEntryView_LastStoredTime(t *testing.T) {
	entryView := EntryView{}
	entryView.lastStoredTime = 5
	if result := entryView.LastStoredTime(); result != 5 {
		t.Fatal("entryView LastStoredTime failed")
	}
}

func TestDataEntryView_LastUpdateTime(t *testing.T) {
	entryView := EntryView{}
	entryView.lastUpdateTime = 5
	if result := entryView.LastUpdateTime(); result != 5 {
		t.Fatal("entryView LastUpdateTime failed")
	}
}

func TestDataEntryView_Ttl(t *testing.T) {
	entryView := EntryView{}
	entryView.ttl = 5
	if result := entryView.Ttl(); result != 5 {
		t.Fatal("entryView Ttl failed")
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

func TestError_CauseClassName(t *testing.T) {
	err := Error{}
	err.causeClassName = "test"
	if result := err.CauseClassName(); result != "test" {
		t.Fatal("error failed")
	}
}

func TestError_ClassName(t *testing.T) {
	err := Error{}
	err.className = "test"
	if result := err.ClassName(); result != "test" {
		t.Fatal("error failed")
	}
}

func TestError_ErrorCode(t *testing.T) {
	err := Error{}
	err.errorCode = 5
	if result := err.ErrorCode(); result != 5 {
		t.Fatal("error failed")
	}
}

func TestError_Message(t *testing.T) {
	err := Error{}
	err.message = "test"
	if result := err.Message(); result != "test" {
		t.Fatal("error failed")
	}
}

func TestError_CauseErrorCode(t *testing.T) {
	err := Error{}
	err.causeErrorCode = 5
	if result := err.CauseErrorCode(); result != 5 {
		t.Fatal("error failed")
	}
}

func TestError_Error(t *testing.T) {
	err := Error{}
	err.message = "test"
	if result := err.Error(); result != "test" {
		t.Fatal("error failed")
	}
}

func TestStackTraceElement_DeclaringClass(t *testing.T) {
	traceElemet := StackTraceElement{}
	traceElemet.declaringClass = "test"
	if result := traceElemet.DeclaringClass(); result != "test" {
		t.Fatal("stackTraceElement failed")
	}
}

func TestStackTraceElement_FileName(t *testing.T) {
	traceElemet := StackTraceElement{}
	traceElemet.fileName = "test"
	if result := traceElemet.FileName(); result != "test" {
		t.Fatal("stackTraceElement failed")
	}
}

func TestStackTraceElement_MethodName(t *testing.T) {
	traceElemet := StackTraceElement{}
	traceElemet.methodName = "test"
	if result := traceElemet.MethodName(); result != "test" {
		t.Fatal("stackTraceElement failed")
	}
}

func TestStackTraceElement_LineNumber(t *testing.T) {
	traceElemet := StackTraceElement{}
	traceElemet.lineNumber = 5
	if result := traceElemet.LineNumber(); result != 5 {
		t.Fatal("stackTraceElement failed")
	}
}

func TestEntryEvent_Key(t *testing.T) {
	entryEvent := EntryEvent{}
	entryEvent.key = "test"
	if result := entryEvent.Key(); result != "test" {
		t.Fatal("entryEvent failed")
	}
}

func TestEntryEvent_MergingValue(t *testing.T) {
	entryEvent := EntryEvent{}
	entryEvent.mergingValue = "test"
	if result := entryEvent.MergingValue(); result != "test" {
		t.Fatal("entryEvent failed")
	}
}

func TestEntryEvent_Uuid(t *testing.T) {
	entryEvent := EntryEvent{}
	testString := "test"
	entryEvent.uuid = &testString
	if result := entryEvent.Uuid(); *result != testString {
		t.Fatal("entryEvent failed")
	}
}

func TestEntryEvent_OldValue(t *testing.T) {
	entryEvent := EntryEvent{}
	entryEvent.oldValue = "test"
	if result := entryEvent.OldValue(); result != "test" {
		t.Fatal("entryEvent failed")
	}
}

func TestEntryEvent_Value(t *testing.T) {
	entryEvent := EntryEvent{}
	entryEvent.value = "test"
	if result := entryEvent.Value(); result != "test" {
		t.Fatal("entryEvent failed")
	}
}

func TestEntryEvent_EventType(t *testing.T) {
	entryEvent := EntryEvent{}
	entryEvent.eventType = 1
	if result := entryEvent.EventType(); result != 1 {
		t.Fatal("entryEvent failed")
	}
}

func TestMapEvent_EventType(t *testing.T) {
	mapEvent := MapEvent{}
	mapEvent.eventType = 5
	if result := mapEvent.EventType(); result != 5 {
		t.Fatal("mapEvent failed")
	}
}

func TestMapEvent_Uuid(t *testing.T) {
	mapEvent := MapEvent{}
	testString := "test"
	mapEvent.uuid = &testString
	if result := mapEvent.Uuid(); *result != testString {
		t.Fatal("mapEvent failed")
	}
}

func TestMapEvent_NumberOfAffectedEntries(t *testing.T) {
	mapEvent := MapEvent{}
	mapEvent.numberOfAffectedEntries = 5
	if result := mapEvent.NumberOfAffectedEntries(); result != 5 {
		t.Fatal("mapEvent failed")
	}
}

func TestError_StackTrace(t *testing.T) {
	err1 := Error{}
	traceElement1 := StackTraceElement{}
	traceElement1.lineNumber = 5
	err1.stackTrace = make([]*StackTraceElement, 1)
	err1.stackTrace[0] = &traceElement1
	result := err1.StackTrace()
	if len(result) != 1 {
		t.Fatal("error StackTrace failed")
	}

	if result[0].LineNumber() != 5 {
		t.Fatal("error StackTrace failed")
	}

}
