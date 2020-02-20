package internal

import "github.com/hazelcast/hazelcast-go-client/internal/proto"

var currentFrame  = &proto.Frame{}
var writeOffset = -1

func (c *Connection)writeTo(dst Buffer, clientMessage *proto.ClientMessage) bool {

	if currentFrame == nil {
		currentFrame = clientMessage.StartFrame()
	}

	for {
		isLastFrame := currentFrame.Next() == nil
		if c.writeFrame(dst, currentFrame, isLastFrame) {
			writeOffset = -1
			if isLastFrame {
				currentFrame = nil
				return true
			}
			currentFrame = currentFrame.Next()
		} else {
			return false
		}
	}


}

func (c *Connection)writeFrame(dst Buffer, frame *proto.Frame, isLastFrame bool) bool {

	bytesWritable := len(dst.buf)
	var frameContentLength int

	if frame.Content == nil {
		frameContentLength = 0
	} else {
		frameContentLength = len(frame.Content)
	}

	//if write offset is -1 put the length and flags byte first
	if writeOffset == -1 {
		if bytesWritable >= proto.SizeOfFrameLengthAndFlags {
			proto.WriteInt32(dst.buf, dst.position, int32(frameContentLength +proto.SizeOfFrameLengthAndFlags),false)
			dst.position = dst.position + proto.IntSizeInBytes

			if isLastFrame {
				proto.WriteInt16(dst.buf, int32(dst.position), (int16)( frame.Flags | uint8(proto.IsFinalFlag)), false)
			} else {
				proto.WriteInt16(dst.buf, int32(dst.position), (int16)(frame.Flags) , false)
			}
			dst.position = dst.position + proto.Int16SizeInBytes
			writeOffset = 0
		} else {
			return false
		}
	}
	bytesWritable = len(dst.buf) - dst.position  //remaining()
	if frame.Content == nil {
		return true
	}

	// the number of bytes that need to be written
	bytesNeeded := frameContentLength - writeOffset

	var bytesWrite int
	var done bool
	if bytesWritable >= bytesNeeded {
		// all bytes for the value are available
		bytesWrite = bytesNeeded
		done = true
	} else {
		// not all bytes for the value are available. Write as much as is available
		bytesWrite = bytesWritable
		done = false
	}

	dst.put(frame.Content, writeOffset, bytesWrite)

	/*8*/ //todo
	remainingLen := clientMessage.FrameLength() //todo
	writeIndex := 0
	for remainingLen > 0 {
		writtenLen, err := c.socket.Write(clientMessage.StartFrame().Content[writeIndex:])
		if err != nil {
			return false
		}
		remainingLen -= writtenLen
		writeIndex += writtenLen
	}
	/*8*/


	writeOffset += bytesWrite

	return done

}
