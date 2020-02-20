package internal

import "github.com/hazelcast/hazelcast-go-client/internal/proto"

var currentFrame  = &proto.Frame{}
var writeOffset = -1

func writeTo(dst Buffer, clientMessage *proto.ClientMessage) bool {

	if currentFrame == nil {
		currentFrame = clientMessage.StartFrame()
	}

	for {
		isLastFrame := currentFrame.Next() == nil
		if writeFrame(dst, currentFrame, isLastFrame) {
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

func writeFrame(dst Buffer, frame *proto.Frame, isLastFrame bool) bool {

	bytesWritable := len(dst.buf)
	var framecontentLength int

	if frame.Content == nil {
		framecontentLength = 0
	} else {
		framecontentLength = len(frame.Content)
	}

	//if write offset is -1 put the length and flags byte first
	if writeOffset == -1 {
		if bytesWritable >= proto.SizeOfFrameLengthAndFlags {
			proto.WriteInt32(dst.buf, dst.position, int32(framecontentLength +proto.SizeOfFrameLengthAndFlags),false)
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
	bytesNeeded := framecontentLength - writeOffset

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



	writeOffset += bytesWrite

	return done

}
