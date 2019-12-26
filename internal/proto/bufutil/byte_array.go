package bufutil

func ByteArrayCodecEncode(clientMessage *ClientMessagex, bytes []byte) {
	clientMessage.Add(&Frame{bytes})
}

func ByteArrayCodecDecode(iterator *ForwardFrameIterator) []byte {
	return iterator.Next().Content
}
