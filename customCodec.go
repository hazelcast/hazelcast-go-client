package hazelcast

/*
Address Codec
*/
func AddressCodecEncode(msg *ClientMessage, address *Address) {
	msg.AppendString(address.Host)
	msg.AppendInt(address.Port)
}
func AddressCodecDecode(msg *ClientMessage) *Address {
	host := msg.ReadString()
	port := msg.ReadInt32()
	return &Address{*host, int(port)}
}

/*
DistributedObjectInfo Codec
*/

func DistributedObjectInfoCodecDecode(msg *ClientMessage) *DistributedObjectInfo {
	serviceName := msg.ReadString()
	name := msg.ReadString()
	return &DistributedObjectInfo{*name, *serviceName}
}

/*
Member Codec
*/

func MemberCodecDecode(msg *ClientMessage) *Member {
	address := AddressCodecDecode(msg)
	uuid := msg.ReadString()
	liteMember := msg.ReadBool()
	attributeSize := msg.ReadInt32()
	attributes := make(map[string]string)
	for i := 0; i < int(attributeSize); i++ {
		key := msg.ReadString()
		value := msg.ReadString()
		attributes[*key] = *value
	}
	return &Member{*address, *uuid, liteMember, attributes}
}
func EntryViewCodecDecode(msg *ClientMessage) *EntryView {
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

func UuidCodecEncode(msg *ClientMessage, uuid Uuid) {
	msg.AppendInt64(uuid.Msb)
	msg.AppendInt64(uuid.Lsb)
}

func UuidCodecDecode(msg *ClientMessage) *Uuid {
	return &Uuid{msg.ReadInt64(), msg.ReadInt64()}
}

/*
	Error Codec
*/
type StackTraceElement struct {
	declaringClass string
	methodName     string
	fileName       string
	lineNumber     int32
}

func ErrorCodec(msg *ClientMessage) *Error {
	response := Error{}
	response.ErrorCode = msg.ReadInt32()
	response.ClassName = *msg.ReadString()
	if !msg.ReadBool() {
		response.Message = *msg.ReadString()
	}
	stackTrace := make([]StackTraceElement, 0)
	stackTraceCount := msg.ReadInt32()
	for i := 0; i < int(stackTraceCount); i++ {
		stackTrace = append(stackTrace, *DecodeStackTrace(msg))
	}
	response.StackTrace = stackTrace
	response.CauseErrorCode = msg.ReadInt32()
	if !msg.ReadBool() {
		response.CauseClassName = *msg.ReadString()
	}
	return &response

}
func DecodeStackTrace(msg *ClientMessage) *StackTraceElement {
	declaringClass := msg.ReadString()
	methodName := msg.ReadString()
	fileName := ""
	if !msg.ReadBool() {
		fileName = *msg.ReadString()
	}
	lineNumber := msg.ReadInt32()
	return &StackTraceElement{*declaringClass, *methodName, fileName, lineNumber}
}
