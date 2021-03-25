package cluster

import (
	"bytes"
	"encoding/binary"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
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
	src                *bytes.Buffer
	clientMessage      *proto.ClientMessage
	readHeader         bool
	currentFrameLength uint32
	currentFlags       uint16
	remainingCap       int
}

func newClientMessageReader() *clientMessageReader {
	return &clientMessageReader{
		src:          bytes.NewBuffer(make([]byte, 0, bufferSize)),
		remainingCap: bufferSize,
	}
}

func (c *clientMessageReader) Append(buf []byte) {
	c.src.Write(buf)
	c.remainingCap -= len(buf)
}

func (c *clientMessageReader) Read() *proto.ClientMessage {
	for {
		if c.readFrame() {
			if c.clientMessage.EndFrame.IsFinalFrame() {
				return c.clientMessage
			}
		} else {
			return nil
		}
	}
}
func (c *clientMessageReader) readFrame() bool {
	if !c.readHeader {
		if c.remainingCap < proto.SizeOfFrameLengthAndFlags {
			c.resetBuffer()
		}
		if c.src.Len() < proto.SizeOfFrameLengthAndFlags {
			// we don't have even the frame length and flags ready
			return false
		}
		frameLength := binary.LittleEndian.Uint32(c.src.Next(proto.IntSizeInBytes))
		if frameLength < proto.SizeOfFrameLengthAndFlags {
			panic("frame length is less than SizeOfFrameLengthAndFlags")
		}
		c.currentFrameLength = frameLength
		c.currentFlags = binary.LittleEndian.Uint16(c.src.Next(2))
		c.readHeader = true
	}
	if c.readHeader {
		size := int(c.currentFrameLength) - proto.SizeOfFrameLengthAndFlags
		if c.remainingCap < size {
			c.resetBuffer()
		}
		if c.src.Len() < size {
			return false
		}
		frameContent := c.src.Next(size)
		frame := proto.NewFrameWith(frameContent, c.currentFlags)
		if c.clientMessage == nil {
			c.clientMessage = proto.NewClientMessageForDecode(frame)
		} else {
			c.clientMessage.AddFrame(frame)
		}
		c.readHeader = false
		return true
	}
	return false
}

func (c *clientMessageReader) Reset() {
	c.clientMessage = nil
}

func (c *clientMessageReader) resetBuffer() {
	// read the remaining data
	all := c.src.Next(c.src.Len())
	// reset the buffer
	c.src.Reset()
	// write the remaining data back
	c.src.Write(all)
}
