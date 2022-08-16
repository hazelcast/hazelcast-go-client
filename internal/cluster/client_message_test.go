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
		{name: "readFramesInMultipleCallsToRead", f: readFramesInMultipleCallsToRead},
		{name: "readFramesInMultipleCallsToReadFromWhenLastPieceIsSmall", f: readFramesInMultipleCallsToReadFromWhenLastPieceIsSmall},
		{name: "readMultiFrameMessage", f: readMultiFrameMessage},
		{name: "readSingleFrameMessage", f: readSingleFrameMessage},
		{name: "readWhenTheFrameLengthAndFlagsNotReceivedAtFirst", f: readWhenTheFrameLengthAndFlagsNotReceivedAtFirst},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.f(t)
		})
	}
}

func readSingleFrameMessage(t *testing.T) {
	// ported from: com.hazelcast.client.impl.protocol.util.ClientMessageReaderTest#testReadSingleFrameMessage
	frame := createFrameWithRandomBytes(t, 42)
	msg := proto.NewClientMessageForEncode()
	msg.AddFrame(frame)
	buf := writeToBuffer(t, msg)
	r := newClientMessageReader()
	r.Append(buf.Bytes())
	require.NotNil(t, r.Read())
	iter := proto.NewForwardFrameIterator(r.clientMessage.Frames)
	require.True(t, iter.HasNext())
	require.Equal(t, frame.Content, iter.Next().Content)
	require.False(t, iter.HasNext())
}

func readMultiFrameMessage(t *testing.T) {
	// ported from: com.hazelcast.client.impl.protocol.util.ClientMessageReaderTest#testReadMultiFrameMessage
	frame1 := createFrameWithRandomBytes(t, 10)
	frame2 := createFrameWithRandomBytes(t, 20)
	frame3 := createFrameWithRandomBytes(t, 30)
	msg := proto.NewClientMessageForEncode()
	msg.AddFrame(frame1)
	msg.AddFrame(frame2)
	msg.AddFrame(frame3)
	buf := writeToBuffer(t, msg)
	r := newClientMessageReader()
	r.Append(buf.Bytes())
	require.NotNil(t, r.Read())
	iter := r.clientMessage.FrameIterator()
	require.True(t, iter.HasNext())
	require.Equal(t, frame1.Content, iter.Next().Content)
	require.True(t, iter.HasNext())
	require.Equal(t, frame2.Content, iter.Next().Content)
	require.True(t, iter.HasNext())
	require.Equal(t, frame3.Content, iter.Next().Content)
	require.False(t, iter.HasNext())
}

func readFramesInMultipleCallsToRead(t *testing.T) {
	// ported from: com.hazelcast.client.impl.protocol.util.ClientMessageReaderTest#testReadFramesInMultipleCallsToReadFrom
	frame := createFrameWithRandomBytes(t, 1000)
	msg := proto.NewClientMessageForEncode()
	msg.AddFrame(frame)
	buf := writeToBuffer(t, msg)
	// split message two part and send sequentially
	part1 := buf.Next(750)
	part2 := buf.Bytes()
	part1buf := bytes.NewBuffer(part1)
	part2buf := bytes.NewBuffer(part2)
	r := newClientMessageReader()
	r.Append(part1buf.Bytes())
	// should not finish reading
	require.Nil(t, r.Read())
	r.Append(part2buf.Bytes())
	// should read remaining bytes and finish reading
	require.NotNil(t, r.Read())
	iter := r.clientMessage.FrameIterator()
	require.True(t, iter.HasNext())
	require.Equal(t, frame.Content, iter.Next().Content)
	require.False(t, iter.HasNext())
}

func readWhenTheFrameLengthAndFlagsNotReceivedAtFirst(t *testing.T) {
	// ported from: com.hazelcast.client.impl.protocol.util.ClientMessageReaderTest#testRead_whenTheFrameLengthAndFlagsNotReceivedAtFirst
	frame := createFrameWithRandomBytes(t, 100)
	msg := proto.NewClientMessage(frame)
	buf := writeToBuffer(t, msg)
	r := newClientMessageReader()
	// should not be able to read with just 4 bytes of data
	r.Append(buf.Bytes()[:4])
	require.Nil(t, r.Read())
	// should be able to read when the rest of the data comes
	r.Append(buf.Bytes()[4:buf.Len()])
	require.NotNil(t, r.Read())
	iter := r.clientMessage.FrameIterator()
	require.True(t, iter.HasNext())
	require.Equal(t, frame.Content, iter.Next().Content)
	require.False(t, iter.HasNext())
}

func readFramesInMultipleCallsToReadFromWhenLastPieceIsSmall(t *testing.T) {
	// ported from: com.hazelcast.client.impl.protocol.util.ClientMessageReaderTest#testReadFramesInMultipleCallsToReadFrom_whenLastPieceIsSmall
	frame := createFrameWithRandomBytes(t, 1000)
	msg := proto.NewClientMessage(frame)
	buf := writeToBuffer(t, msg)
	// Message Length = 1000 + 6 bytes
	// part1 = 750, part2 = 252, part3 = 4 bytes
	part1 := buf.Next(750)
	part2 := buf.Next(252)
	part3 := buf.Bytes()
	part1Buf := bytes.NewBuffer(part1)
	part2Buf := bytes.NewBuffer(part2)
	part3Buf := bytes.NewBuffer(part3)
	// create a r and send message part by part
	r := newClientMessageReader()
	r.Append(part1Buf.Bytes())
	require.Nil(t, r.Read())
	r.Append(part2Buf.Bytes())
	require.Nil(t, r.Read())
	// it should only be able to read after part 3
	r.Append(part3Buf.Bytes())
	require.NotNil(t, r.Read())
	iter := r.clientMessage.FrameIterator()
	require.Equal(t, frame.Content, iter.Next().Content)
	require.False(t, iter.HasNext())
}

func createFrameWithRandomBytes(t *testing.T, bytes int) proto.Frame {
	// ported from: com.hazelcast.client.impl.protocol.util.ClientMessageReaderTest#createFrameWithRandomBytes
	content := make([]byte, bytes)
	_, err := rand.Read(content)
	require.NoError(t, err)
	return proto.NewFrame(content)
}

func writeToBuffer(t *testing.T, msg *proto.ClientMessage) *bytes.Buffer {
	// ported from: com.hazelcast.client.impl.protocol.util.ClientMessageReaderTest#writeToBuffer
	buf := bytes.NewBuffer(make([]byte, 0, msg.TotalLength()))
	err := msg.Write(buf)
	require.NoError(t, err)
	return buf
}
