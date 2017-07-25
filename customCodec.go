package hazelcast
/*
Address Codec
 */
func  AddresCodecEncode(msg *ClientMessage,address *Address){
	msg.AppendString(address.Host)
	msg.AppendInt(address.Port)
}
func AddressCodecDecode(msg *ClientMessage) *Address{
	host :=msg.ReadString()
	port :=msg.ReadInt()
	return &Address{*host,int(port)}
}
func (address *Address) CalculateSize() int{
	dataSize := 0
	dataSize += CalculateSizeString(&address.Host)
	dataSize += INT_SIZE_IN_BYTES
	return dataSize
}
/*
DistributedObjectInfo Codec
 */
func DistributedObjectInfoCodecEncode(msg *ClientMessage,obj *DistributedObjectInfo){
	msg.AppendString(obj.ServiceName)
	msg.AppendString(obj.Name)
}
func DistributedObjectInfoCodecDecode(msg *ClientMessage) *DistributedObjectInfo {
	serviceName := msg.ReadString()
	name := msg.ReadString()
	return &DistributedObjectInfo{*name,*serviceName}
}
func (obj *DistributedObjectInfo) CalculateSize() int{
	dataSize := 0
	dataSize += CalculateSizeString(&obj.Name)
	dataSize += CalculateSizeString(&obj.ServiceName)
	return dataSize
}

/*
Member Codec
 */
func MemberCodecEncode (msg *ClientMessage,member *Member){
	AddresCodecEncode(msg,&member.Address)
	msg.AppendString(member.Uuid)
	msg.AppendBool(member.IsLiteMember)
	msg.AppendInt(len(member.Attributes))
	for key,value := range member.Attributes {
		msg.AppendString(key)
		msg.AppendString(value)
	}
}
func MemberCodecDecode (msg *ClientMessage) *Member {
	address :=AddressCodecDecode(msg)
	uuid :=msg.ReadString()
	liteMember :=msg.ReadBool()
	attributeSize :=msg.ReadInt()
	attributes := make(map[string]string)
	for i := 0 ; i < int(attributeSize) ; i++ {
		key := msg.ReadString()
		value := msg.ReadString()
		attributes[*key] = *value
	}
	return &Member{*address,*uuid,liteMember,attributes}
}
func (member *Member) CalculateSize()int {
	dataSize := 0
	dataSize += member.Address.CalculateSize()
	dataSize += CalculateSizeString(&member.Uuid)
	dataSize += BOOLEAN_SIZE_IN_BYTES
	dataSize += INT_SIZE_IN_BYTES //Size of the map(attributes)
	for key,value := range member.Attributes {
		dataSize += CalculateSizeString(&key)
		dataSize += CalculateSizeString(&value)
	}
	return dataSize
}
func EntryViewCodecEncode (msg *ClientMessage,entryView *EntryView){
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
func EntryViewCodecDecode (msg * ClientMessage) *EntryView{
	entryView := EntryView{}
	entryView.Key = msg.ReadData()
	entryView.Value = msg.ReadData()
	entryView.Cost = msg.ReadInt64()
	entryView.CreationTime = msg.ReadInt64()
	entryView.ExpirationTime = msg.ReadInt64()
	entryView.Hits = msg.ReadInt64()
	entryView.LastAccessTime = msg.ReadInt64()
	entryView.LastStoredTime = msg.ReadInt64()
	entryView.LastUpdateTime = msg.ReadInt64()
	entryView.Version = msg.ReadInt64()
	entryView.EvictionCriteriaNumber = msg.ReadInt64()
	entryView.Ttl = msg.ReadInt64()
	return &entryView
}
func (ev *EntryView) CalculateSize() int{
	dataSize := 0
	dataSize += ev.Key.CalculateSize()
	dataSize += ev.Value.CalculateSize()
	dataSize += 10*INT64_SIZE_IN_BYTES
	return dataSize
}


