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
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/protocol/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

func TestAddressCodecEncodeDecode(t *testing.T) {
	host := "test-host"
	var port int32 = 8080
	address := Address{host, port}
	msg := NewClientMessage(nil, addressCalculateSize(&address))
	AddressCodecEncode(msg, &address)
	//Skip the header.
	for i := 0; i < len(READ_HEADER); i++ {
		msg.ReadUint8()
	}
	if result := AddressCodecDecode(msg); *result != address {
		t.Error("AddressCodecDecode returned a wrong host or port")
	}
}

func TestDistributedObjectInfoCodecEncodeDecode(t *testing.T) {
	name := "test-name"
	serviceName := "test-serviceName"
	distributedObjectInfo := DistributedObjectInfo{name, serviceName}
	msg := NewClientMessage(nil, DistributedObjectInfoCalculateSize(&distributedObjectInfo))
	DistributedObjectInfoCodecEncode(msg, &distributedObjectInfo)
	//Skip the header.
	for i := 0; i < len(READ_HEADER); i++ {
		msg.ReadUint8()
	}
	if result := DistributedObjectInfoCodecDecode(msg); *result != distributedObjectInfo {
		t.Error("DistributedObjectInfoCodecDecode returned a wrong name or service name")
	}
}

func TestMemberCodecEncodeDecode(t *testing.T) {
	address := Address{"test-host", 8080}
	uuid := "test-uuid"
	isLiteMember := true
	attributes := make(map[string]string)
	attributes["key1"] = "value1"
	attributes["key2"] = "value2"
	member := Member{address, uuid, isLiteMember, attributes}
	msg := NewClientMessage(nil, MemberCalculateSize(&member))
	MemberCodecEncode(msg, &member)
	//Skip the header.
	for i := 0; i < len(READ_HEADER); i++ {
		msg.ReadUint8()
	}
	if result := MemberCodecDecode(msg); !result.Equal(member) {
		t.Error("MemberCodecDecode returned a wrong member")
	}
}

func TestDataEntryViewCodecEncodeDecode(t *testing.T) {
	key := "test-key"
	value := "test-value"
	entryView := DataEntryView{}
	entryView.keyData = &serialization.Data{Payload: []byte(key)}
	entryView.valueData = &serialization.Data{Payload: []byte(value)}
	entryView.cost = 123123
	entryView.creationTime = 1212
	entryView.expirationTime = 12
	entryView.hits = 1235
	entryView.lastAccessTime = 1232
	entryView.lastStoredTime = 1236
	entryView.lastUpdateTime = 1236
	entryView.version = 1
	entryView.evictionCriteriaNumber = 122
	entryView.ttl = 14555
	msg := NewClientMessage(nil, DataEntryViewCalculateSize(&entryView))
	DataEntryViewCodecEncode(msg, &entryView)
	//Skip the header.
	for i := 0; i < len(READ_HEADER); i++ {
		msg.ReadUint8()
	}
	if result := DataEntryViewCodecDecode(msg); !result.Equal(entryView) {
		t.Error("EntryViewCodecDecode returned a wrong member")
	}

}

/*
	Helper functions
*/
/*
	EntryView helper functions
*/
func DataEntryViewCodecEncode(msg *ClientMessage, entryView *DataEntryView) {
	msg.AppendData(entryView.keyData)
	msg.AppendData(entryView.valueData)
	msg.AppendInt64(entryView.cost)
	//msg.AppendInt64(entryView.Cost)
	msg.AppendInt64(entryView.creationTime)
	msg.AppendInt64(entryView.expirationTime)
	msg.AppendInt64(entryView.hits)
	msg.AppendInt64(entryView.lastAccessTime)
	msg.AppendInt64(entryView.lastStoredTime)
	msg.AppendInt64(entryView.lastUpdateTime)
	msg.AppendInt64(entryView.version)
	msg.AppendInt64(entryView.evictionCriteriaNumber)
	msg.AppendInt64(entryView.ttl)
}

func DataEntryViewCalculateSize(ev *DataEntryView) int {
	dataSize := 0
	dataSize += dataCalculateSize(ev.keyData)
	dataSize += dataCalculateSize(ev.valueData)
	dataSize += 10 * bufutil.Int64SizeInBytes
	return dataSize
}

/*
	Member helper functions
*/
func MemberCodecEncode(msg *ClientMessage, member *Member) {
	AddressCodecEncode(msg, &member.address)
	msg.AppendString(&member.uuid)
	msg.AppendBool(member.isLiteMember)
	msg.AppendInt32(int32(len(member.attributes)))
	for key, value := range member.attributes {
		msg.AppendString(&key)
		msg.AppendString(&value)
	}
}

func MemberCalculateSize(member *Member) int {
	dataSize := 0
	dataSize += addressCalculateSize(&member.address)
	dataSize += stringCalculateSize(&member.uuid)
	dataSize += bufutil.BoolSizeInBytes
	dataSize += bufutil.Int32SizeInBytes //Size of the map(attributes)
	for key, value := range member.attributes {
		dataSize += stringCalculateSize(&key)
		dataSize += stringCalculateSize(&value)
	}
	return dataSize
}

/*
	DistributedObjectInfo Helper functions
*/
func DistributedObjectInfoCodecEncode(msg *ClientMessage, obj *DistributedObjectInfo) {
	msg.AppendString(&obj.serviceName)
	msg.AppendString(&obj.name)
}

func DistributedObjectInfoCalculateSize(obj *DistributedObjectInfo) int {
	dataSize := 0
	dataSize += stringCalculateSize(&obj.name)
	dataSize += stringCalculateSize(&obj.serviceName)
	return dataSize
}
