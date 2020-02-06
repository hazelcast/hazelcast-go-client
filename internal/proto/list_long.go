package proto

func ListLongArrayCodecEncode(clientMessage *ClientMessage, long []int64) {
	itemCount := len(long)
	frame := &Frame{Content: make([]byte, itemCount *LongSizeInBytes)}
	var iterator []int64
	for i := 0; i < itemCount; i++ {
		EncodeLong(frame.Content, int32(i *LongSizeInBytes), iterator[i]) //itarator.next
	}
	clientMessage.Add(frame)
}

func ListLongArrayCodecDecode(frame *Frame) []int64 {
	var  itemCount int
	if frame.Content == nil{
		itemCount = 0
	}else{
		itemCount = len(frame.Content) / LongSizeInBytes
	}
	result := make([]int64,itemCount)
	for i := 0; i < itemCount; i++ {
		result = append(result, DecodeLong(frame.Content, int32(i*LongSizeInBytes)))
	}
	return result
}

func ListLongArrayCodecDecodeFrame(iterator *ForwardFrameIterator) []int64 {
	return LongArrayCodecDecode(iterator.Next())
}
