package bufutil

func ByteArrayCodecEncode(clientMessage *ClientMessagex, bytes []byte) {
	clientMessage.Add(&Frame{bytes})
}

func ByteArrayCodecDecode(frame *Frame) []byte {
	return frame.Content
}

func ByteArrayCodecDecodeFrame(iterator *ForwardFrameIterator) []byte {
	return ByteArrayCodecDecode(iterator.Next())
}
