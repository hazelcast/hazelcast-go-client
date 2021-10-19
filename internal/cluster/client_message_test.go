package cluster

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestUnFragmentedMessage(t *testing.T) {
	uuid := types.NewUUID()
	request := codec.EncodeClientAuthenticationRequest(
		"dev",
		"user",
		"pass",
		uuid,
		internal.ClientType,
		byte(serializationVersion),
		internal.ClientVersion,
		"go",
		nil,
	)
	reader := newClientMessageReader()
	err := request.Write(reader.src)
	assert.Nil(t, err)
	msg, noPreviousFragment := reader.Read()
	assert.False(t, noPreviousFragment)
	assert.NotNil(t, msg)
	assert.Equal(t, msg.Type(), codec.ClientAuthenticationCodecRequestMessageType)
	assert.True(t, len(msg.Frames) == len(request.Frames))
	for ind, expectedFrame := range request.Frames {
		assert.True(t, bytes.Compare(expectedFrame.Content, msg.Frames[ind].Content) == 0)
	}
}

// extends buf with a frame encoding given string
func encodeString(buf []byte, str string, isFinal bool) []byte {
	extension := make([]byte, proto.SizeOfFrameLengthAndFlags)
	payload := []byte(str)
	binary.LittleEndian.PutUint32(extension, uint32(proto.SizeOfFrameLengthAndFlags+len(payload)))
	if isFinal {
		binary.LittleEndian.PutUint16(extension[proto.IntSizeInBytes:], proto.IsFinalFlag)
	}
	buf = append(buf, append(extension, payload...)...)
	return buf
}

func TestFragmentedMessage(t *testing.T) {
	size := proto.SizeOfFrameLengthAndFlags + proto.LongSizeInBytes
	fragmentID := 123
	beginBuf := make([]byte, size)
	binary.LittleEndian.PutUint32(beginBuf, uint32(size))
	binary.LittleEndian.PutUint16(beginBuf[proto.IntSizeInBytes:], proto.BeginFragmentFlag)
	binary.LittleEndian.PutUint64(beginBuf[proto.SizeOfFrameLengthAndFlags:], uint64(fragmentID))
	beginBuf = encodeString(beginBuf, "a", true)

	middleBuf := make([]byte, size)
	binary.LittleEndian.PutUint32(middleBuf, uint32(size))
	binary.LittleEndian.PutUint64(middleBuf[proto.SizeOfFrameLengthAndFlags:], uint64(fragmentID))
	middleBuf = encodeString(middleBuf, "b", true)

	endBuf := make([]byte, size)
	binary.LittleEndian.PutUint32(endBuf, uint32(size))
	binary.LittleEndian.PutUint16(endBuf[proto.IntSizeInBytes:], proto.EndFragmentFlag)
	binary.LittleEndian.PutUint64(endBuf[proto.SizeOfFrameLengthAndFlags:], uint64(fragmentID))
	endBuf = encodeString(endBuf, "c", true)

	reader := newClientMessageReader()
	reader.Append(beginBuf)
	msg, noPreviousFragment := reader.Read()
	assert.False(t, noPreviousFragment)
	assert.Nil(t, msg)

	reader.Append(middleBuf)
	msg, noPreviousFragment = reader.Read()
	assert.False(t, noPreviousFragment)
	assert.Nil(t, msg)

	reader.Append(endBuf)
	msg, noPreviousFragment = reader.Read()
	assert.False(t, noPreviousFragment)
	assert.NotNil(t, msg)
	var accumulate []byte
	for _, frame := range msg.Frames {
		accumulate = append(accumulate, frame.Content...)
	}
	assert.Equal(t, "abc", string(accumulate))
}
