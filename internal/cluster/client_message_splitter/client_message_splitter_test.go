package client_message_splitter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

func TestGetSubFramesWithFragmentation(t *testing.T) {
	msg := codec.RandomClientAuthenticationRequestMessage(1000)
	iterator := msg.FrameIterator()
	fragments := GetFragments(128, msg)
	assert.Equal(t, 19, len(fragments))
	assertFragments(t, fragments, iterator)
}

func TestGetSubFramesWithNoFragmentation(t *testing.T) {
	msg := codec.RandomClientAuthenticationRequestMessage(1000)
	iterator := msg.FrameIterator()
	fragments := GetFragments(4000, msg)
	assertFragments(t, fragments, iterator)
}

func assertFragments(t *testing.T, fragments []*proto.ClientMessage, originalIterator *proto.ForwardFrameIterator) {
	for _, fragment := range fragments {
		iterator := fragment.FrameIterator()
		//skip fragmentation header
		iterator.Next()
		for iterator.HasNext() {
			actualFrame := iterator.Next()
			expectedFrame := originalIterator.Next()
			assert.Equal(t, expectedFrame.GetLength(), actualFrame.GetLength())
			assert.Equal(t, expectedFrame.Flags, actualFrame.Flags)
			assert.Equal(t, expectedFrame.Content, actualFrame.Content)
		}
	}
}
