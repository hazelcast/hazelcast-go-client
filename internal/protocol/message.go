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
	"encoding/binary"
	"unicode/utf8"

	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

// ClientMessage is the carrier framed data as defined below.
// Any request parameter, response or event data will be carried in the payload.
//	0                   1                   2                   3
//	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|R|                      Frame Length                           |
//	+-------------+---------------+---------------------------------+
//	|  Version    |B|E|  Flags    |               Type              |
//	+-------------+---------------+---------------------------------+
//	|                                                               |
//	+                       CorrelationId                           +
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                        PartitionId                            |
//	+-----------------------------+---------------------------------+
//	|        Data Offset          |                                 |
//	+-----------------------------+                                 |
//	|                      Message Payload Data                    ...
//	|                                                              ...
type ClientMessage struct {
	Buffer      []byte
	writeIndex  int32
	readIndex   int32
	IsRetryable bool
}

/*

	Constructor

*/

func NewClientMessage(buffer []byte, payloadSize int) *ClientMessage {
	clientMessage := new(ClientMessage)
	if buffer != nil {
		//Message has a buffer so it will be decoded.
		clientMessage.Buffer = buffer
		clientMessage.readIndex = common.HeaderSize
	} else {
		//Client message that will be encoded.
		clientMessage.Buffer = make([]byte, common.HeaderSize+payloadSize)
		clientMessage.SetDataOffset(common.HeaderSize)
		clientMessage.writeIndex = common.HeaderSize
	}
	clientMessage.IsRetryable = false
	return clientMessage
}

func (msg *ClientMessage) FrameLength() int32 {
	return int32(binary.LittleEndian.Uint32(msg.Buffer[common.FrameLengthFieldOffset:common.VersionFieldOffset]))
}

func (msg *ClientMessage) SetFrameLength(v int32) {
	binary.LittleEndian.PutUint32(msg.Buffer[common.FrameLengthFieldOffset:common.VersionFieldOffset], uint32(v))
}

func (msg *ClientMessage) SetVersion(v uint8) {
	msg.Buffer[common.VersionFieldOffset] = byte(v)
}

func (msg *ClientMessage) Flags() uint8 {
	return msg.Buffer[common.FlagsFieldOffset]
}

func (msg *ClientMessage) SetFlags(v uint8) {
	msg.Buffer[common.FlagsFieldOffset] = byte(v)
}
func (msg *ClientMessage) AddFlags(v uint8) {
	msg.Buffer[common.FlagsFieldOffset] = msg.Buffer[common.FlagsFieldOffset] | byte(v)
}

func (msg *ClientMessage) HasFlags(flags uint8) uint8 {
	value := msg.Flags() & flags
	if value == flags {
		return value
	}
	return 0
}

func (msg *ClientMessage) MessageType() common.MessageType {
	return common.MessageType(binary.LittleEndian.Uint16(msg.Buffer[common.TypeFieldOffset:common.CorrelationIdFieldOffset]))
}

func (msg *ClientMessage) SetMessageType(v common.MessageType) {
	binary.LittleEndian.PutUint16(msg.Buffer[common.TypeFieldOffset:common.CorrelationIdFieldOffset], uint16(v))
}

func (msg *ClientMessage) CorrelationId() int64 {
	return int64(binary.LittleEndian.Uint64(msg.Buffer[common.CorrelationIdFieldOffset:common.PartitionIdFieldOffset]))
}

func (msg *ClientMessage) SetCorrelationId(val int64) {
	binary.LittleEndian.PutUint64(msg.Buffer[common.CorrelationIdFieldOffset:common.PartitionIdFieldOffset], uint64(val))
}

func (msg *ClientMessage) PartitionId() int32 {
	return int32(binary.LittleEndian.Uint32(msg.Buffer[common.PartitionIdFieldOffset:common.DataOffsetFieldOffset]))
}

func (msg *ClientMessage) SetPartitionId(val int32) {
	binary.LittleEndian.PutUint32(msg.Buffer[common.PartitionIdFieldOffset:common.DataOffsetFieldOffset], uint32(val))
}

func (msg *ClientMessage) DataOffset() uint16 {
	return binary.LittleEndian.Uint16(msg.Buffer[common.DataOffsetFieldOffset:common.HeaderSize])
}

func (msg *ClientMessage) SetDataOffset(v uint16) {
	binary.LittleEndian.PutUint16(msg.Buffer[common.DataOffsetFieldOffset:common.HeaderSize], v)
}

func (msg *ClientMessage) writeOffset() int32 {
	return int32(msg.DataOffset()) + msg.writeIndex
}

func (msg *ClientMessage) readOffset() int32 {
	return msg.readIndex
}

/*
	PAYLOAD
*/

func (msg *ClientMessage) AppendByte(v uint8) {
	msg.Buffer[msg.writeIndex] = byte(v)
	msg.writeIndex += common.ByteSizeInBytes
}
func (msg *ClientMessage) AppendUint8(v uint8) {
	msg.Buffer[msg.writeIndex] = byte(v)
	msg.writeIndex += common.ByteSizeInBytes
}
func (msg *ClientMessage) AppendInt32(v int32) {
	binary.LittleEndian.PutUint32(msg.Buffer[msg.writeIndex:msg.writeIndex+common.Int32SizeInBytes], uint32(v))
	msg.writeIndex += common.Int32SizeInBytes
}
func (msg *ClientMessage) AppendData(v *serialization.Data) {
	msg.AppendByteArray(v.Buffer())
}

func (msg *ClientMessage) AppendByteArray(arr []byte) {
	length := int32(len(arr))
	//length
	msg.AppendInt32(length)
	//copy content
	copy(msg.Buffer[msg.writeIndex:msg.writeIndex+length], arr)
	msg.writeIndex += length
}
func (msg *ClientMessage) AppendInt64(v int64) {
	binary.LittleEndian.PutUint64(msg.Buffer[msg.writeIndex:msg.writeIndex+common.Int64SizeInBytes], uint64(v))
	msg.writeIndex += common.Int64SizeInBytes
}

func (msg *ClientMessage) AppendString(str *string) {
	if utf8.ValidString(*str) {
		msg.AppendByteArray([]byte(*str))
	} else {
		buff := make([]byte, 0, len(*str)*3)
		n := 0
		for _, b := range *str {
			n += utf8.EncodeRune(buff[n:], rune(b))
		}
		//append fixed size slice
		msg.AppendByteArray(buff[0:n])
	}
}

func (msg *ClientMessage) AppendBool(v bool) {
	if v {
		msg.AppendByte(1)
	} else {
		msg.AppendByte(0)
	}
}

/*
	PAYLOAD READ
*/

func (msg *ClientMessage) ReadInt32() int32 {
	int := int32(binary.LittleEndian.Uint32(msg.Buffer[msg.readOffset() : msg.readOffset()+common.Int32SizeInBytes]))
	msg.readIndex += common.Int32SizeInBytes
	return int
}
func (msg *ClientMessage) ReadInt64() int64 {
	int64 := int64(binary.LittleEndian.Uint64(msg.Buffer[msg.readOffset() : msg.readOffset()+common.Int64SizeInBytes]))
	msg.readIndex += common.Int64SizeInBytes
	return int64
}
func (msg *ClientMessage) ReadUint8() uint8 {
	byte := byte(msg.Buffer[msg.readOffset()])
	msg.readIndex += common.ByteSizeInBytes
	return byte
}

func (msg *ClientMessage) ReadBool() bool {
	if msg.ReadUint8() == 1 {
		return true
	} else {
		return false
	}
}
func (msg *ClientMessage) ReadString() *string {
	str := string(msg.ReadByteArray())
	return &str
}
func (msg *ClientMessage) ReadData() *serialization.Data {
	return &serialization.Data{msg.ReadByteArray()}
}
func (msg *ClientMessage) ReadByteArray() []byte {
	length := msg.ReadInt32()
	result := msg.Buffer[msg.readOffset() : msg.readOffset()+length]
	msg.readIndex += length
	return result
}

/*
	Helpers
*/
func (msg *ClientMessage) UpdateFrameLength() {
	msg.SetFrameLength(int32(msg.writeIndex))
}
func (msg *ClientMessage) Accumulate(newMsg *ClientMessage) {
	start := newMsg.DataOffset()
	end := newMsg.FrameLength()
	msg.Buffer = append(msg.Buffer, newMsg.Buffer[start:end]...)
	msg.SetFrameLength(int32(len(msg.Buffer)))
}

func (msg *ClientMessage) IsComplete() bool {
	return (msg.readOffset() >= common.HeaderSize) && (msg.readOffset() == msg.FrameLength())
}
