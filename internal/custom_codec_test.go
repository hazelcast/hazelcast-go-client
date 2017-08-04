package internal

import "testing"

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
	entryView.Key = Data{[]byte(key)}
	entryView.Value = Data{[]byte(value)}
	entryView.Cost = 123123
	entryView.CreationTime = 1212
	entryView.ExpirationTime = 12
	entryView.Hits = 1235
	entryView.LastAccessTime = 1232
	entryView.LastStoredTime = 1236
	entryView.LastUpdateTime = 1236
	entryView.Version = 1
	entryView.EvictionCriteriaNumber = 122
	entryView.Ttl = 14555
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
	msg.AppendData(entryView.Key)
	msg.AppendData(entryView.Value)
	msg.AppendInt64(entryView.Cost)
	//msg.AppendInt64(entryView.Cost)
	msg.AppendInt64(entryView.CreationTime)
	msg.AppendInt64(entryView.ExpirationTime)
	msg.AppendInt64(entryView.Hits)
	msg.AppendInt64(entryView.LastAccessTime)
	msg.AppendInt64(entryView.LastStoredTime)
	msg.AppendInt64(entryView.LastUpdateTime)
	msg.AppendInt64(entryView.Version)
	msg.AppendInt64(entryView.EvictionCriteriaNumber)
	msg.AppendInt64(entryView.Ttl)
}
func EntryViewCalculateSize(ev *EntryView) int {
	dataSize := 0
	dataSize += ev.Key.CalculateSize()
	dataSize += ev.Value.CalculateSize()
	dataSize += 10 * INT64_SIZE_IN_BYTES
	return dataSize
}

/*
	Member helper functions
*/
func MemberCodecEncode(msg *ClientMessage, member *Member) {
	AddressCodecEncode(msg, &member.Address)
	msg.AppendString(member.Uuid)
	msg.AppendBool(member.IsLiteMember)
	msg.AppendInt(len(member.Attributes))
	for key, value := range member.Attributes {
		msg.AppendString(key)
		msg.AppendString(value)
	}
}
func MemberCalculateSize(member *Member) int {
	dataSize := 0
	dataSize += AddressCalculateSize(&member.Address)
	dataSize += StringCalculateSize(&member.Uuid)
	dataSize += BOOL_SIZE_IN_BYTES
	dataSize += INT_SIZE_IN_BYTES //Size of the map(attributes)
	for key, value := range member.Attributes {
		dataSize += StringCalculateSize(&key)
		dataSize += StringCalculateSize(&value)
	}
	return dataSize
}

/*
	DistributedObjectInfo Helper functions
*/
func DistributedObjectInfoCodecEncode(msg *ClientMessage, obj *DistributedObjectInfo) {
	msg.AppendString(obj.ServiceName)
	msg.AppendString(obj.Name)
}
func DistributedObjectInfoCalculateSize(obj *DistributedObjectInfo) int {
	dataSize := 0
	dataSize += StringCalculateSize(&obj.Name)
	dataSize += StringCalculateSize(&obj.ServiceName)
	return dataSize
}
