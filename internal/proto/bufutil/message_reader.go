package bufutil
/*

import (
	"bytes"
	"log"
)

const IntMaxValue = 0x7fffffff
	const IntMask  = 0xffff
	var maxMessageLength int32
	var readOffset    = -1
	var clientMessage ClientMessagex
	var sumUntrustedMessageLength int32

func ClientMessageReader(maxMessageLen int32){
	if maxMessageLen > 0 {
		maxMessageLength = maxMessageLen
	}else{
		maxMessageLength = IntMaxValue
	}
}

func readFrom(src bytes.Buffer, trusted bool) bool {
	for {
		if readFrame(src, trusted) {
			if IsFlagSet(clientMessage.EndFrame.Flags, IsFinalFlag) {
				return true
			}
			readOffset = -1
		} else {
			return false
		}
	}
}

func readFrame(src bytes.Buffer, trusted bool) bool {
	remaining := src.Len()
	var frameLength int32
	if remaining < SizeOfFrameLengthAndFlags {
		return false
	}
	if readOffset == -1 {
		frameLength = ReadInt32(src.Bytes(), &src[off], false)

	if frameLength < SizeOfFrameLengthAndFlags {
		log.Fatal(ErrorCodeIllegalArgument, "The client message frame reported illegal length (%d bytes). Minimal length is the size of frame header (%d bytes).", frameLength, SizeOfFrameLengthAndFlags)
	}
	if !trusted {
		if (IntMaxValue-frameLength) < sumUntrustedMessageLength || (sumUntrustedMessageLength+frameLength) > maxMessageLength {
			log.Fatal("The client message size (%d + %d) exceededs the maximum allowed length (%d)", sumUntrustedMessageLength, frameLength, maxMessageLength)
		}
		sumUntrustedMessageLength += frameLength
	}

	src.position(src.position() + IntSizeInBytes)
	flags := ReadUInt8(src.Bytes(), src.position()) & IntMask
	src.position(src.position() + Uint8SizeInBytes)
	size := frameLength - SizeOfFrameLengthAndFlags
	bytes := make([]byte, size)
	frame := &Frame{
		Content: bytes,
		Flags:   flags,
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
	return accumulate(src, frame.Content, len(frame.Content) - readOffset)
}

func accumulate(src bytes.Buffer, dest []byte, length int) bool {
	 remaining := src.Len()
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


*/