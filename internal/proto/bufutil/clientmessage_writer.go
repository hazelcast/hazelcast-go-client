package bufutil


var currentFrame  = &Frame{}
var writeOffset = -1

func writeTo(dst Buffer, clientMessage *ClientMessage) bool{

	if currentFrame == nil {
		currentFrame = clientMessage.startFrame
	}

	for ; ;  {
		isLastFrame := currentFrame.next == nil
		if writeFrame(dst, currentFrame, isLastFrame) {
			writeOffset = -1
			if isLastFrame {
				currentFrame = nil
				return true
			}
			currentFrame = currentFrame.next
		} else {
			return false
		}
	}

}

func writeFrame(dst Buffer, frame *Frame , isLastFrame bool) bool {

	bytesWritable := len(dst.buf)
	var framecontentLength int

	if frame.content == nil {
		framecontentLength = 0
	} else {
		framecontentLength = len(frame.content)
	}

	//if write offset is -1 put the length and flags byte first
	if writeOffset == -1 {
		if bytesWritable >= SizeOfFrameLengthAndFlags {
			WriteInt32(dst.buf, dst.position, int32(framecontentLength + SizeOfFrameLengthAndFlags),false)
			dst.position = dst.position + IntSizeInBytes

			if isLastFrame {
				WriteInt16(dst.buf, int32(dst.position), (int16)( int32(frame.flags) | IsFinalFlag), false)
			} else {
				WriteInt16(dst.buf, int32(dst.position), (int16)(frame.flags) , false)
			}
			dst.position = dst.position + Int16SizeInBytes
			writeOffset = 0
		} else {
			return false
		}
	}
	bytesWritable = len(dst.buf) - dst.position  //remaining()
	if frame.content == nil {
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

	dst.put(frame.content, writeOffset, bytesWrite)
	writeOffset += bytesWrite

	return done

}
