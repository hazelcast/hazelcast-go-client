package bufutil

func LongArrayCodecEncode(clientMessage *ClientMessage, arr []int64) {
	itemCount := len(arr)
	frame := &Frame{content: make([]byte, itemCount * LongSizeInBytes)}
	for i := 0 ; i < itemCount; i++ {
		EncodeLong(frame.content, int32(i*LongSizeInBytes), arr[i])
	}
	clientMessage.Add(frame)
}

func LongArrayCodecDecode(frame *Frame) []int64 {
	itemCount := len(frame.content) / LongSizeInBytes
	result := make([]int64,itemCount)
	for i := 0 ; i < itemCount; i++ {
		result[i] = DecodeLong(frame.content, int32(i*LongSizeInBytes))
	}
	return result
}

func LongArrayCodecDecodeFrame(iterator *ForwardFrameIterator) []int64 {
	return LongArrayCodecDecode(iterator.Next())
}
