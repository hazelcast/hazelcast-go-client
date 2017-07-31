package hazelcast

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
