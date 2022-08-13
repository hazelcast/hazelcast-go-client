package cluster

import (
	"bytes"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestClientMessageReader(t *testing.T) {
	testCases := []struct {
		f    func(t *testing.T)
		name string
	}{
		{name: "readSingleFrame", f: readSingleFrame},
		{name: "readMultiFrameMessage", f: readMultiFrameMessage},
		{name: "readFramesInMultipleCallsToRead", f: readFramesInMultipleCallsToRead},
		{name: "readWhenTheFrameLengthAndFlagsNotReceivedAtFirst", f: readWhenTheFrameLengthAndFlagsNotReceivedAtFirst},
		{name: "readFramesInMultipleCallsToReadFromWhenLastPieceIsSmall", f: readFramesInMultipleCallsToReadFromWhenLastPieceIsSmall},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.f(t)
		})
	}
}

func readSingleFrame(t *testing.T) {
	frame := createFrameWithRandomBytes(t, 42)
	message := proto.NewClientMessageForEncode()
	message.AddFrame(frame)
	buffer := writeToBuffer(t, message)
	reader := newClientMessageReader()
	reader.Append(buffer.Bytes())
	require.NotNil(t, reader.Read())
	iterator := proto.NewForwardFrameIterator(reader.clientMessage.Frames)
	require.True(t, iterator.HasNext())
	frameRead := iterator.Next()
	require.Equal(t, frame.Content, frameRead.Content)
	require.False(t, iterator.HasNext())
}

func readMultiFrameMessage(t *testing.T) {
	frame1 := createFrameWithRandomBytes(t, 10)
	frame2 := createFrameWithRandomBytes(t, 20)
	frame3 := createFrameWithRandomBytes(t, 30)
	message := proto.NewClientMessageForEncode()
	message.AddFrame(frame1)
	message.AddFrame(frame2)
	message.AddFrame(frame3)
	buffer := writeToBuffer(t, message)
	reader := newClientMessageReader()
	reader.Append(buffer.Bytes())
	require.NotNil(t, reader.Read())
	iterator := reader.clientMessage.FrameIterator()
	require.True(t, iterator.HasNext())
	frameRead := iterator.Next()
	require.Equal(t, frame1.Content, frameRead.Content)
	require.True(t, iterator.HasNext())
	frameRead = iterator.Next()
	require.Equal(t, frame2.Content, frameRead.Content)
	require.True(t, iterator.HasNext())
	frameRead = iterator.Next()
	require.Equal(t, frame3.Content, frameRead.Content)
	require.False(t, iterator.HasNext())
}

func readFramesInMultipleCallsToRead(t *testing.T) {
	frame := createFrameWithRandomBytes(t, 1000)
	message := proto.NewClientMessageForEncode()
	message.AddFrame(frame)
	buffer := writeToBuffer(t, message)
	// split message two part and send sequentially
	part1 := buffer.Next(750)
	part2 := buffer.Bytes()
	part1buffer := bytes.NewBuffer(part1)
	part2buffer := bytes.NewBuffer(part2)
	reader := newClientMessageReader()
	reader.Append(part1buffer.Bytes())
	// should not finish reading
	require.Nil(t, reader.Read())
	reader.Append(part2buffer.Bytes())
	// should read remaining bytes and finish reading
	require.NotNil(t, reader.Read())
	iterator := reader.clientMessage.FrameIterator()
	require.True(t, iterator.HasNext())
	frameRead := iterator.Next()
	require.Equal(t, frame.Content, frameRead.Content)
	require.False(t, iterator.HasNext())
}

func readWhenTheFrameLengthAndFlagsNotReceivedAtFirst(t *testing.T) {
	frame := createFrameWithRandomBytes(t, 100)
	message := proto.NewClientMessage(frame)
	buffer := writeToBuffer(t, message)
	reader := newClientMessageReader()
	// should not be able to read with just 4 bytes of data
	reader.Append(buffer.Bytes()[:4])
	require.Nil(t, reader.Read())
	// should be able to read when the rest of the data comes
	reader.Append(buffer.Bytes()[4:buffer.Len()])
	require.NotNil(t, reader.Read())
	iterator := reader.clientMessage.FrameIterator()
	require.True(t, iterator.HasNext())
	frameRead := iterator.Next()
	require.Equal(t, frame.Content, frameRead.Content)
	require.False(t, iterator.HasNext())
}

func readFramesInMultipleCallsToReadFromWhenLastPieceIsSmall(t *testing.T) {
	frame := createFrameWithRandomBytes(t, 1000)
	message := proto.NewClientMessage(frame)
	buffer := writeToBuffer(t, message)
	// Message Length = 1000 + 6 bytes
	// part1 = 750, part2 = 252, part3 = 4 bytes
	part1 := buffer.Next(750)
	part2 := buffer.Next(252)
	part3 := buffer.Bytes()
	part1Buffer := bytes.NewBuffer(part1)
	part2Buffer := bytes.NewBuffer(part2)
	part3Buffer := bytes.NewBuffer(part3)
	// create a reader and send message part by part
	reader := newClientMessageReader()
	reader.Append(part1Buffer.Bytes())
	require.Nil(t, reader.Read())
	reader.Append(part2Buffer.Bytes())
	require.Nil(t, reader.Read())
	// it should only be able to read after part 3
	reader.Append(part3Buffer.Bytes())
	require.NotNil(t, reader.Read())
	iterator := reader.clientMessage.FrameIterator()
	frameRead := iterator.Next()
	require.Equal(t, frame.Content, frameRead.Content)
	require.False(t, iterator.HasNext())
}

func createFrameWithRandomBytes(t *testing.T, bytes int) proto.Frame {
	content := make([]byte, bytes)
	_, err := rand.Read(content)
	require.NoError(t, err)
	return proto.NewFrame(content)
}

func writeToBuffer(t *testing.T, message *proto.ClientMessage) *bytes.Buffer {
	buffer := bytes.NewBuffer(make([]byte, 0, message.TotalLength()))
	err := message.Write(buffer)
	require.NoError(t, err)
	return buffer
}
