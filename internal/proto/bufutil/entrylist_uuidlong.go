package bufutil

func EntryListUUIDLongCodec() {

}
const EntrySizeInBytes = UUIDSizeInBytes + LongSizeInBytes
//TODO: WIP
func EntryListUUIDLongCodecEncode(clientMessage *ClientMessage, collection []*Pair)  {
	itemCount := len(collection)
	frame := &Frame{content:make([]byte, itemCount * EntrySizeInBytes)}

	for i, it := range collection{
		EncodeUUID(frame.content, int32(i*EntrySizeInBytes), it.Key().(Uuid))
		EncodeLong(frame.content, int32(i*EntrySizeInBytes+UUIDSizeInBytes), it.Value().(int64))
	}

	clientMessage.Add(frame)
}

func EntryListUUIDLongCodecDecode(iterator *ForwardFrameIterator) []*Pair  {
	frame := iterator.Next()
	itemCount := len(frame.content) / EntrySizeInBytes

	var result []*Pair

	for  i := 0; i < itemCount; i++ {
		 key := DecodeUUID(frame.content, int32(i*EntrySizeInBytes))
		 value := DecodeLong(frame.content, int32(i*EntrySizeInBytes+UUIDSizeInBytes))
		result[i] = NewPair(key,value)

	}
	return result
}
