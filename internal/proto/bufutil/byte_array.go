package bufutil

func ByteArrayCodecEncode(clientMessage *ClientMessage, bytes []byte) {
	clientMessage.Add(&Frame{content:bytes})
}

func ByteArrayCodecDecode(iterator *ForwardFrameIterator) []byte {
	return iterator.Next().content
}
