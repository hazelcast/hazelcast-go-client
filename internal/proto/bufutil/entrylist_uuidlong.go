package bufutil

func EntryListUUIDLongCodec() {

}
const EntrySizeInBytes = UUIDSizeInBytes + LongSizeInBytes
//TODO: WIP
func EntryListUUIDLongCodecEncode(clientMessage *ClientMessage, collection []map[string]int64)  {
	itemCount := len(collection)
	frame := &Frame{Content:make([]byte, itemCount * EntrySizeInBytes)}
	iterator := collection
	for i := 0; i < itemCount; i++ {
		entry := make(map[string]int64)
		EncodeUUID(frame.Content, int32(i*EntrySizeInBytes), entry.k)
		EncodeLong(frame.Content, int32(i*EntrySizeInBytes+UUIDSizeInBytes), entry.v)
	}
	clientMessage.Add(frame)
}

func EntryListUUIDLongCodecDecode(iterator *ForwardFrameIterator) []map[string]int64  {
	frame := iterator.Next()
	itemCount := len(frame.Content) / EntrySizeInBytes
	var result []map[string]int64
	for  i := 0; i < itemCount; i++ {
		var key = DecodeUUID(frame.Content, int32(i*EntrySizeInBytes))
		var value = DecodeLong(frame.Content, int32(i*EntrySizeInBytes+UUIDSizeInBytes))
		var absMap  map[string]int64
		absMap[key] = value
		result = append(result,absMap)
	}
	return result
}
