package protocol

import (
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/serialization"
	"testing"
)

func TestAddressCodecEncodeDecode(t *testing.T) {
	host := "test-host"
	port := 8080
	address := Address{host, port}
	msg := NewClientMessage(nil, AddressCalculateSize(&address))
	AddressCodecEncode(msg, &address)
	//Skip the header.
	for i := 0; i < len(READ_HEADER); i++ {
		msg.ReadUint8()
	}
	if result := AddressCodecDecode(msg); *result != address {
		t.Errorf("AddressCodecDecode returned a wrong host or port")
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
		t.Errorf("DistributedObjectInfoCodecDecode returned a wrong name or service name")
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
		t.Errorf("MemberCodecDecode returned a wrong member")
	}
}
func TestEntryViewCodecEncodeDecode(t *testing.T) {
	key := "test-key"
	value := "test-value"
	entryView := EntryView{}
	entryView.key = Data{[]byte(key)}
	entryView.value = Data{[]byte(value)}
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
	msg := NewClientMessage(nil, EntryViewCalculateSize(&entryView))
	EntryViewCodecEncode(msg, &entryView)
	//Skip the header.
	for i := 0; i < len(READ_HEADER); i++ {
		msg.ReadUint8()
	}
	if result := EntryViewCodecDecode(msg); !result.Equal(entryView) {
		t.Errorf("EntryViewCodecDecode returned a wrong member")
	}

}

/*
	Helper functions
*/
/*
	EntryView helper functions
*/
func EntryViewCodecEncode(msg *ClientMessage, entryView *EntryView) {
	msg.AppendData(&entryView.key)
	msg.AppendData(&entryView.value)
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
func EntryViewCalculateSize(ev *EntryView) int {
	dataSize := 0
	dataSize += DataCalculateSize(&ev.key)
	dataSize += DataCalculateSize(&ev.value)
	dataSize += 10 * INT64_SIZE_IN_BYTES
	return dataSize
}

/*
	Member helper functions
*/
func MemberCodecEncode(msg *ClientMessage, member *Member) {
	AddressCodecEncode(msg, &member.address)
	msg.AppendString(&member.uuid)
	msg.AppendBool(member.isLiteMember)
	msg.AppendInt(len(member.attributes))
	for key, value := range member.attributes {
		msg.AppendString(&key)
		msg.AppendString(&value)
	}
}
func MemberCalculateSize(member *Member) int {
	dataSize := 0
	dataSize += AddressCalculateSize(&member.address)
	dataSize += StringCalculateSize(&member.uuid)
	dataSize += BOOL_SIZE_IN_BYTES
	dataSize += INT_SIZE_IN_BYTES //Size of the map(attributes)
	for key, value := range member.attributes {
		dataSize += StringCalculateSize(&key)
		dataSize += StringCalculateSize(&value)
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
	dataSize += StringCalculateSize(&obj.name)
	dataSize += StringCalculateSize(&obj.serviceName)
	return dataSize
}
