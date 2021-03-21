package cluster

import (
	"bytes"
	"encoding/binary"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"sync"
)

type clientMessageBuilder struct {
	incompleteMessages map[int64]*proto.ClientMessage
	handleResponse     func(msg *proto.ClientMessage)
}

func (mb *clientMessageBuilder) onMessage(msg *proto.ClientMessage) {
	if msg.StartFrame.HasUnFragmentedMessageFlags() {
		mb.handleResponse(msg)
	} else {
		println("here")
	}

	//TODO
	/**
		if msg.HasFlags(bufutil.BeginEndFlag) > 0 {
		mb.handleResponse(msg)
	} else if msg.HasFlags(bufutil.BeginFlag) > 0 {
		mb.incompleteMessages[msg.CorrelationID()] = msg
	} else {
		message, found := mb.incompleteMessages[msg.CorrelationID()]
		if !found {
			return
		}
		message.Accumulate(msg)
		if msg.HasFlags(bufutil.EndFlag) > 0 {
			message.AddFlags(bufutil.BeginEndFlag)
			mb.handleResponse(message)
			delete(mb.incompleteMessages, msg.CorrelationID())
		}
	}
	*/

}

type clientMessageReader struct {
	src           *bytes.Buffer
	readOffset    int
	clientMessage *proto.ClientMessage
	rwMutex       sync.RWMutex
}

func newClientMessageReader() *clientMessageReader {
	return &clientMessageReader{src: &bytes.Buffer{}, readOffset: -1}
}

func (c *clientMessageReader) Append(buffer *bytes.Buffer) {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	c.src = buffer
}

func (c *clientMessageReader) Read() *proto.ClientMessage {
	for {
		if c.readFrame() {
			if c.clientMessage.EndFrame.IsFinalFrame() {
				return c.clientMessage
			}
			c.readOffset = -1
		} else {
			return nil
		}
	}
}
func (c *clientMessageReader) readFrame() bool {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	// init internal buffer
	remaining := c.src.Len()
	if remaining < proto.SizeOfFrameLengthAndFlags {
		// we don't have even the frame length and flags ready
		return false
	}

	if c.readOffset == -1 {
		frameLength := binary.LittleEndian.Uint32(c.src.Next(proto.IntSizeInBytes))
		if frameLength < proto.SizeOfFrameLengthAndFlags {
			//TODO add exception
		}

		flags := binary.LittleEndian.Uint16(c.src.Bytes())
		c.src.Next(proto.ShortSizeInBytes)
		size := frameLength - proto.SizeOfFrameLengthAndFlags
		frame := proto.NewFrameWith(make([]byte, size), flags)
		if c.clientMessage == nil {
			c.clientMessage = proto.NewClientMessageForDecode(frame)
		} else {
			c.clientMessage.AddFrame(frame)
		}
		c.readOffset = 0
		if size == 0 {
			return true
		}
	}

	length := len(c.clientMessage.EndFrame.Content) - c.readOffset
	return c.accumulate(c.src, length)
}

func (c *clientMessageReader) accumulate(src *bytes.Buffer, length int) bool {
	remaining := src.Len()
	readLength := length
	if remaining < length {
		readLength = remaining
	}

	if readLength > 0 {
		end := c.readOffset + readLength
		for i := c.readOffset; i < end; i++ {
			c.clientMessage.EndFrame.Content[i], _ = src.ReadByte()
		}
		c.readOffset += readLength
		return readLength == length
	}

	return false
}

func (c *clientMessageReader) Reset() {
	//c.rwMutex.Lock()
	//defer c.rwMutex.Unlock()
	c.clientMessage = nil
	c.readOffset = -1
}
