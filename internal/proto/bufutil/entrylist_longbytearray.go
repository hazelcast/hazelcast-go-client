package bufutil

func EntryListLongByteArrayCodec() {
	
}

func EntryListLongByteArrayCodecEncode(clientMessage *ClientMessage, M []map[int64][]byte)  {
	var valueList []int64
	clientMessage.Add(BeginFrame.Copy())
	for k, v := range M{
		valueList = append(valueList, int64(k))
		ByteArrayCodecEncode(clientMessage,v[int64(k)]) //TODO
	}
	clientMessage.Add(EndFrame.Copy())
	ListLongArrayCodecEncode(clientMessage, valueList)
}

func EntryListLongByteArrayCodecDecode(iterator *ForwardFrameIterator,
	decodeKeyFunc func(messageK *ForwardFrameIterator) interface{}, decodeValueFunc func(messageV *ForwardFrameIterator) interface{}) []map[interface{}]interface{}  {
	var result []map[interface{}]interface{}
	var absMap = make(map[interface{}]interface{})
	//begin frame, map
	iterator.Next()
	for !NextFrameIsDataStructureEndFrame(iterator) {
		key := decodeKeyFunc(iterator)
		value := decodeValueFunc(iterator)
		absMap[key]=value
		result = append(result, absMap)
	}
	iterator.Next()
	return result
}
