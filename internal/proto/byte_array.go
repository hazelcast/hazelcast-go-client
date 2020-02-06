package proto

func ByteArrayCodecEncode(clientMessage *ClientMessage, bytes []byte) {
	clientMessage.Add(&Frame{Content: bytes})
}

func ByteArrayCodecDecode(iterator *ForwardFrameIterator) []byte {
	return iterator.Next().Content
}
