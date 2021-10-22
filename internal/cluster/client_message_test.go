package cluster

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster/client_message_splitter"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/types"
)

var (
	clientMsg1 = codec.RandomClientAuthenticationRequestMessage(1000)
	clientMsg2 = codec.RandomClientAuthenticationRequestMessage(1000)
)

func TestSplitAndBuildClientMessage(t *testing.T) {
	reader := newClientMessageReader()
	fragments := client_message_splitter.GetFragments(128, clientMsg1)
	for _, fragment := range fragments {
		_ = fragment.Write(reader.src) // writes to byte buffer, no error
	}
	var resultingMsg *proto.ClientMessage
	resultingMsg = readUntilMessageOrRetryCount(reader, 19) // we have 19 fragments, corresponds to each read
	assert.NotNil(t, resultingMsg)
	assert.Equal(t, len(clientMsg1.Frames), len(resultingMsg.Frames))
	assert.Equal(t, clientMsg1.TotalLength(), resultingMsg.TotalLength())
}

func TestFragmentFieldAccess(t *testing.T) {
	fragments := client_message_splitter.GetFragments(128, clientMsg1)
	firstMsg := fragments[0]
	iterator := firstMsg.FrameIterator()
	iterator.Next() // Skip the first frame as it is the fragmentation frame
	startFrameContent := iterator.Next().Content
	assert.Equal(t, codec.ClientAuthenticationCodecRequestMessageType, int32(binary.LittleEndian.Uint32(startFrameContent[proto.TypeFieldOffset:])))
	assert.Equal(t, clientMsg1.CorrelationID(), int64(binary.LittleEndian.Uint64(startFrameContent[proto.CorrelationIDFieldOffset:])))
}

func TestSplitAndBuildMultipleMessages(t *testing.T) {
	reader := newClientMessageReader()
	fragments1 := client_message_splitter.GetFragments(128, clientMsg1)
	fragments2 := client_message_splitter.GetFragments(128, clientMsg2)
	for ind, fragment1 := range fragments1 {
		_ = fragment1.Write(reader.src)       // writes to byte buffer, no error
		_ = fragments2[ind].Write(reader.src) // writes to byte buffer, no error
	}

	msg1 := readUntilMessageOrRetryCount(reader, 37)
	msg2 := readUntilMessageOrRetryCount(reader, 1)

	assert.NotNil(t, msg1)
	assert.Equal(t, len(clientMsg1.Frames), len(msg2.Frames))
	assertMessagesEqual(t, clientMsg1, msg1)
	assertMessagesEqual(t, clientMsg2, msg2)
}

func TestSplitAndBuildMessageSmallerThanFrameSize(t *testing.T) {
	reader := newClientMessageReader()
	fragments := client_message_splitter.GetFragments(100000, clientMsg1)
	for _, fragment := range fragments {
		_ = fragment.Write(reader.src) // writes to byte buffer, no error
	}
	var resultingMsg *proto.ClientMessage
	resultingMsg = readUntilMessageOrRetryCount(reader, 1) // we have 19 fragments, corresponds to each read
	assert.NotNil(t, resultingMsg)
	assert.Equal(t, len(clientMsg1.Frames), len(resultingMsg.Frames))
	assert.Equal(t, clientMsg1.TotalLength(), resultingMsg.TotalLength())
}

func readUntilMessageOrRetryCount(reader *clientMessageReader, retryCount int) *proto.ClientMessage {
	for i := 0; i < retryCount; i++ {
		if msg := reader.Read(); msg != nil {
			return msg
		}
	}
	return nil
}

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
	_ = request.Write(reader.src)
	msg := reader.Read()
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
	msg := reader.Read()
	assert.Nil(t, msg)

	reader.Append(middleBuf)
	msg = reader.Read()
	assert.Nil(t, msg)

	reader.Append(endBuf)
	msg = reader.Read()
	assert.NotNil(t, msg)
	var accumulate []byte
	for _, frame := range msg.Frames {
		accumulate = append(accumulate, frame.Content...)
	}
	assert.Equal(t, "abc", string(accumulate))
}

func assertMessagesEqual(t *testing.T, expected, actual *proto.ClientMessage) {
	//these flags related to framing and can differ between two semantically equal messages
	mask := ^(proto.UnfragmentedMessage | proto.IsFinalFlag)
	umask := uint16(mask)
	expIterator := expected.FrameIterator()
	actIterator := actual.FrameIterator()

	for expIterator.HasNext() {
		expectedFrame := expIterator.Next()
		actualFrame := actIterator.Next()
		assert.Equal(t, expectedFrame.GetLength(), actualFrame.GetLength())
		assert.Equal(t, expectedFrame.Flags&umask, actualFrame.Flags&umask)
		assert.Equal(t, expectedFrame.Content, actualFrame.Content)
	}
}
