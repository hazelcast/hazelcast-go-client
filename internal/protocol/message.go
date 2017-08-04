package protocol

import (
	"encoding/binary"
	"unicode/utf8"

	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/serialization"
)

/*
Client Message is the carrier framed data as defined below.
Any request parameter, response or event data will be carried in the payload.
	0                   1                   2                   3
	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	|R|                      Frame Length                           |
	+-------------+---------------+---------------------------------+
	|  Version    |B|E|  Flags    |               Type              |
	+-------------+---------------+---------------------------------+
	|                                                               |
	+                       CorrelationId                           +
	|                                                               |
	+---------------------------------------------------------------+
	|                        PartitionId                            |
	+-----------------------------+---------------------------------+
	|        Data Offset          |                                 |
	+-----------------------------+                                 |
	|                      Message Payload Data                    ...
	|                                                              ...
*/
type ClientMessage struct {
	Buffer      []byte
	writeIndex  int
	readIndex   int
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
		clientMessage.readIndex = HEADER_SIZE
	} else {
		//Client message that will be encoded.
		clientMessage.Buffer = make([]byte, HEADER_SIZE+payloadSize)
		clientMessage.SetDataOffset(HEADER_SIZE)
		clientMessage.writeIndex = HEADER_SIZE
	}
	clientMessage.IsRetryable = false
	return clientMessage
}

func (msg *ClientMessage) FrameLength() int32 {
	return int32(binary.LittleEndian.Uint32(msg.Buffer[FRAME_LENGTH_FIELD_OFFSET:VERSION_FIELD_OFFSET]))
}

func (msg *ClientMessage) SetFrameLength(v int32) {
	binary.LittleEndian.PutUint32(msg.Buffer[FRAME_LENGTH_FIELD_OFFSET:VERSION_FIELD_OFFSET], uint32(v))
}

func (msg *ClientMessage) SetVersion(v uint8) {
	msg.Buffer[VERSION_FIELD_OFFSET] = byte(v)
}

func (msg *ClientMessage) Flags() uint8 {
	return msg.Buffer[FLAGS_FIELD_OFFSET]
}

func (msg *ClientMessage) SetFlags(v uint8) {
	msg.Buffer[FLAGS_FIELD_OFFSET] = byte(v)
}
func (msg *ClientMessage) AddFlags(v uint8) {
	msg.Buffer[FLAGS_FIELD_OFFSET] = msg.Buffer[FLAGS_FIELD_OFFSET] | byte(v)
}

func (msg *ClientMessage) HasFlags(flags uint8) uint8 {
	return msg.Flags() & flags
}

func (msg *ClientMessage) MessageType() MessageType {
	return MessageType(binary.LittleEndian.Uint16(msg.Buffer[TYPE_FIELD_OFFSET:CORRELATION_ID_FIELD_OFFSET]))
}

func (msg *ClientMessage) SetMessageType(v MessageType) {
	binary.LittleEndian.PutUint16(msg.Buffer[TYPE_FIELD_OFFSET:CORRELATION_ID_FIELD_OFFSET], uint16(v))
}

func (msg *ClientMessage) CorrelationId() int64 {
	return int64(binary.LittleEndian.Uint64(msg.Buffer[CORRELATION_ID_FIELD_OFFSET:PARTITION_ID_FIELD_OFFSET]))
}

func (msg *ClientMessage) SetCorrelationId(val int64) {
	binary.LittleEndian.PutUint64(msg.Buffer[CORRELATION_ID_FIELD_OFFSET:PARTITION_ID_FIELD_OFFSET], uint64(val))
}

func (msg *ClientMessage) PartitionId() int32 {
	return int32(binary.LittleEndian.Uint32(msg.Buffer[PARTITION_ID_FIELD_OFFSET:DATA_OFFSET_FIELD_OFFSET]))
}

func (msg *ClientMessage) SetPartitionId(val int32) {
	binary.LittleEndian.PutUint32(msg.Buffer[PARTITION_ID_FIELD_OFFSET:DATA_OFFSET_FIELD_OFFSET], uint32(val))
}

func (msg *ClientMessage) DataOffset() uint16 {
	return binary.LittleEndian.Uint16(msg.Buffer[DATA_OFFSET_FIELD_OFFSET:HEADER_SIZE])
}

func (msg *ClientMessage) SetDataOffset(v uint16) {
	binary.LittleEndian.PutUint16(msg.Buffer[DATA_OFFSET_FIELD_OFFSET:HEADER_SIZE], v)
}

func (msg *ClientMessage) writeOffset() int {
	return int(msg.DataOffset()) + msg.writeIndex
}

func (msg *ClientMessage) readOffset() int {
	return msg.readIndex
}

/*
	PAYLOAD
*/

func (msg *ClientMessage) AppendByte(v uint8) {
	msg.Buffer[msg.writeIndex] = byte(v)
	msg.writeIndex += BYTE_SIZE_IN_BYTES
}
func (msg *ClientMessage) AppendUint8(v uint8) {
	msg.Buffer[msg.writeIndex] = byte(v)
	msg.writeIndex += BYTE_SIZE_IN_BYTES
}
func (msg *ClientMessage) AppendInt(v int) {
	binary.LittleEndian.PutUint32(msg.Buffer[msg.writeIndex:msg.writeIndex+INT_SIZE_IN_BYTES], uint32(v))
	msg.writeIndex += INT_SIZE_IN_BYTES
}
func (msg *ClientMessage) AppendInt32(v int32) {
	binary.LittleEndian.PutUint32(msg.Buffer[msg.writeIndex:msg.writeIndex+INT_SIZE_IN_BYTES], uint32(v))
	msg.writeIndex += INT32_SIZE_IN_BYTES
}
func (msg *ClientMessage) AppendData(v Data) {
	msg.AppendByteArray(v.Buffer)
}

func (msg *ClientMessage) AppendByteArray(arr []byte) {
	length := len(arr)
	//length
	msg.AppendInt(length)
	//copy content
	copy(msg.Buffer[msg.writeIndex:msg.writeIndex+length], arr)
	msg.writeIndex += length
}
func (msg *ClientMessage) AppendInt64(v int64) {
	binary.LittleEndian.PutUint64(msg.Buffer[msg.writeIndex:msg.writeIndex+INT64_SIZE_IN_BYTES], uint64(v))
	msg.writeIndex += INT64_SIZE_IN_BYTES
}

func (msg *ClientMessage) AppendString(str string) {
	if utf8.ValidString(str) {
		msg.AppendByteArray([]byte(str))
	} else {
		buff := make([]byte, 0, len(str)*3)
		n := 0
		for _, b := range str {
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
	int := int32(binary.LittleEndian.Uint32(msg.Buffer[msg.readOffset(): msg.readOffset()+INT_SIZE_IN_BYTES]))
	msg.readIndex += INT_SIZE_IN_BYTES
	return int
}
func (msg *ClientMessage) ReadInt64() int64 {
	int64 := int64(binary.LittleEndian.Uint64(msg.Buffer[msg.readOffset(): msg.readOffset()+INT64_SIZE_IN_BYTES]))
	msg.readIndex += INT64_SIZE_IN_BYTES
	return int64
}
func (msg *ClientMessage) ReadUint8() uint8 {
	byte := byte(msg.Buffer[msg.readOffset()])
	msg.readIndex += BYTE_SIZE_IN_BYTES
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
func (msg *ClientMessage) ReadData() Data {
	return Data{msg.ReadByteArray()}
}
func (msg *ClientMessage) ReadByteArray() []byte {
	length := msg.ReadInt32()
	result := msg.Buffer[msg.readOffset(): msg.readOffset()+int(length)]
	msg.readIndex += int(length)
	return result
}

/*
	Helpers
*/
func (msg *ClientMessage) UpdateFrameLength() {
	msg.SetFrameLength(int32(msg.writeIndex))
}
