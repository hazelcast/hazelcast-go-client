package bufutil


var currentFrame  = &Frame{}
var writeOffset = -1

func writeTo(dst Buffer, clientMessage *ClientMessage) bool{

	if currentFrame == nil {
		currentFrame = clientMessage.StartFrame
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
	var frameContentLength int

	if frame.Content == nil {
		frameContentLength = 0
	} else {
		frameContentLength = len(frame.Content)
	}

	//if write offset is -1 put the length and flags byte first
	if writeOffset == -1 {
		if bytesWritable >= SizeOfFrameLengthAndFlags {
			WriteInt32(dst.buf, dst.position, int32(frameContentLength + SizeOfFrameLengthAndFlags),false)
			dst.position = dst.position + IntSizeInBytes

			if isLastFrame {
				WriteInt16(dst.buf, int32(dst.position), (int16)(frame.Flags | IsFinalFlag), false)
			} else {
				WriteInt16(dst.buf, int32(dst.position), (int16)(frame.Flags) , false)
			}
			dst.position = dst.position + Int16SizeInBytes
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
	writeOffset += bytesWrite

	return done

}
