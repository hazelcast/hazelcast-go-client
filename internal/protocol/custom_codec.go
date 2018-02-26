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

/*
Address Codec
*/
func AddressCodecEncode(msg *ClientMessage, address *Address) {
	msg.AppendString(&address.host)
	msg.AppendInt(address.port)
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
	entryView.key = msg.ReadData()
	entryView.value = msg.ReadData()
	entryView.cost = msg.ReadInt64()
	entryView.creationTime = msg.ReadInt64()
	entryView.expirationTime = msg.ReadInt64()
	entryView.hits = msg.ReadInt64()
	entryView.lastAccessTime = msg.ReadInt64()
	entryView.lastStoredTime = msg.ReadInt64()
	entryView.lastUpdateTime = msg.ReadInt64()
	entryView.version = msg.ReadInt64()
	entryView.evictionCriteriaNumber = msg.ReadInt64()
	entryView.ttl = msg.ReadInt64()
	return &entryView
}
func DataEntryViewCodecDecode(msg *ClientMessage) *DataEntryView {
	dataEntryView := DataEntryView{}
	dataEntryView.keyData = msg.ReadData()
	dataEntryView.valueData = msg.ReadData()
	dataEntryView.cost = msg.ReadInt64()
	dataEntryView.creationTime = msg.ReadInt64()
	dataEntryView.expirationTime = msg.ReadInt64()
	dataEntryView.hits = msg.ReadInt64()
	dataEntryView.lastAccessTime = msg.ReadInt64()
	dataEntryView.lastStoredTime = msg.ReadInt64()
	dataEntryView.lastUpdateTime = msg.ReadInt64()
	dataEntryView.version = msg.ReadInt64()
	dataEntryView.evictionCriteriaNumber = msg.ReadInt64()
	dataEntryView.ttl = msg.ReadInt64()
	return &dataEntryView
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

func ErrorCodecDecode(msg *ClientMessage) *Error {
	response := Error{}
	response.errorCode = msg.ReadInt32()
	response.className = *msg.ReadString()
	if !msg.ReadBool() {
		response.message = *msg.ReadString()
	}
	stackTrace := make([]*StackTraceElement, 0)
	stackTraceCount := msg.ReadInt32()
	for i := 0; i < int(stackTraceCount); i++ {
		stackTrace = append(stackTrace, DecodeStackTrace(msg))
	}
	response.stackTrace = stackTrace
	response.causeErrorCode = msg.ReadInt32()
	if !msg.ReadBool() {
		response.causeClassName = *msg.ReadString()
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
