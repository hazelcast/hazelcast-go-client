package bufutil

func LongArrayCodecEncode(clientMessage *ClientMessagex, arr []int64) {
	itemCount := len(arr)
	frame := &Frame{make([]byte, itemCount * LongSizeInBytes)}
	for i := 0 ; i < itemCount; i++ {
		EncodeLong(frame.Content, int32(i*LongSizeInBytes), arr[i])
	}
	clientMessage.Add(frame)
}

func LongArrayCodecDecode(frame *Frame) []int64 {
	itemCount := len(frame.Content) / LongSizeInBytes
	result := make([]int64,itemCount)
	for i := 0 ; i < itemCount; i++ {
		result[i] = DecodeLong(frame.Content, int32(i*LongSizeInBytes))
	}
	return result
}

func LongArrayCodecDecodeFrame(iterator *ForwardFrameIterator) []int64 {
	return LongArrayCodecDecode(iterator.Next())
}
