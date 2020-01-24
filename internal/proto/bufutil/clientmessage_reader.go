package bufutil

import (
	"log"
)


var clientMessage ClientMessage
var sumUntrustedMessageLength int32
var readOffset = -1
var maxMessageLength int32
const intMaxValue = 0x7fffffff
const intMask     = 0xffff

func ClientMessageReader(maxMessageLen int32){
	if maxMessageLen > 0 {
		maxMessageLength = maxMessageLen
	}else{
		maxMessageLength = intMaxValue
	}
}

func readFrom(src Buffer, trusted bool) bool {
	for {
		if readFrame(src, trusted) {
			if IsFlagSet(clientMessage.endFrame.flags, IsFinalFlag) {
				return true
			}
			readOffset = -1
		} else {
			return false
		}
	}
}

func readFrame(src Buffer, trusted bool) bool {
	remaining := len(src.buf) - src.position
	var frameLength int32
	if remaining < SizeOfFrameLengthAndFlags {
		return false
	}
	if readOffset == -1 {
		frameLength = ReadInt32(src.buf, src.position , false)

		if frameLength < SizeOfFrameLengthAndFlags {
			log.Fatal(ErrorCodeIllegalArgument, "The client message frame reported illegal length (%d bytes). Minimal length is the size of frame header (%d bytes).", frameLength, SizeOfFrameLengthAndFlags)
		}
		if !trusted {
			if (intMaxValue-frameLength) < sumUntrustedMessageLength || (sumUntrustedMessageLength+frameLength) > maxMessageLength {
				log.Fatal("The client message size (%d + %d) exceededs the maximum allowed length (%d)", sumUntrustedMessageLength, frameLength, maxMessageLength)
			}
			sumUntrustedMessageLength += frameLength
		}

		src.position = src.position + IntSizeInBytes
		flags_ := int32(ReadUInt8(src.buf, int32(src.position))) & intMask
		src.position = src.position + Uint8SizeInBytes
		size := frameLength - SizeOfFrameLengthAndFlags
		bytes := make([]byte, size)
		frame := &Frame{
			content: bytes,
			flags:   uint8(flags_),
		}
		if clientMessage.StartFrame == nil {
			clientMessage = CreateForDecode(frame)
		} else {
			clientMessage.Add(frame)
		}
		readOffset = 0
		if size == 0 {
			return true
		}
	}
	frame := EndFrame
	return accumulate(src, frame.content, len(frame.content) - readOffset)
}

func accumulate(src Buffer, dest []byte, length int) bool {
	remaining := len(src.buf) - src.position
	var readLength int
	if remaining < length {
		readLength = remaining
	}else {
		readLength = length
	}
	if readLength > 0 {
		src.get(dest, readOffset, readLength)
		readOffset += readLength
		return readLength == length
	}
	return false
}

