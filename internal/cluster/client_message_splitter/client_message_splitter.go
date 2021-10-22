package client_message_splitter

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type readState int

const (
	// beginning means last fragment is added to client message, need to create new fragment
	beginning readState = iota
	// middle means at least a frame is added to current fragment
	middle
)

// fragmentIDSequence is used as an atomic id counter for the package
var fragmentIDSequence int64

// GetFragments splits given proto.ClientMessage into fragments of maximum size.
func GetFragments(maxFrameSize int, clientMessage *proto.ClientMessage) []*proto.ClientMessage {
	if clientMessage.TotalLength() <= maxFrameSize {
		return []*proto.ClientMessage{clientMessage}
	}
	fragmentID := atomic.AddInt64(&fragmentIDSequence, 1)
	var fragments []*proto.ClientMessage
	iterator := clientMessage.FrameIterator()
	var (
		fragment *proto.ClientMessage
		state    = beginning
		length   = 0
	)
	for iterator.HasNext() {
		frame := iterator.PeekNext()
		frameLen := frame.GetLength()
		length += frameLen

		if frameLen > maxFrameSize {
			iterator.Next()
			if state == middle {
				fragments = append(fragments, fragment)
			}
			fragment = createFragment(fragmentID)
			fragment.AddFrame(frame.Copy())
			fragments = append(fragments, fragment)
			state = beginning
			length = 0
		} else if length <= maxFrameSize {
			iterator.Next()
			if state == beginning {
				fragment = createFragment(fragmentID)
			}
			fragment.AddFrame(frame.Copy())
			state = middle
		} else {
			fragments = append(fragments, fragment)
			state = beginning
			length = 0
		}
	}
	if state == middle {
		fragments = append(fragments, fragment)
	}
	fragments[0].Frames[0].Flags |= proto.BeginFragmentFlag
	fragments[len(fragments)-1].Frames[0].Flags |= proto.EndFragmentFlag
	return fragments
}

func createFragment(fragmentID int64) *proto.ClientMessage {
	fragment := proto.NewClientMessageForEncode()
	frame := proto.NewFrame(make([]byte, proto.LongSizeInBytes))
	binary.LittleEndian.PutUint64(frame.Content[proto.FragmentationIDOffset:], uint64(fragmentID))
	fragment.AddFrame(frame)
	return fragment
}
