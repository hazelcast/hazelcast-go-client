package bufutil

type EntryListCodec struct {

}

func EntryListCodecEncode(clientMessage *ClientMessagex, M []map[interface{}]interface{},
	encodeKeyFunc func(messageK *ClientMessagex, K interface{}), encodeValueFunc func(messageV *ClientMessagex, V interface{}))  {
	clientMessage.Add(BeginFrame.Copy())
	for k, v := range M{
		encodeKeyFunc(clientMessage, k)
		encodeValueFunc(clientMessage, v[k])
	}
	clientMessage.Add(EndFrame.Copy())
}

func EntryListCodecEncodeNullable(clientMessage *ClientMessagex, M []map[interface{}]interface{},
		encodeKeyFunc func(messageK *ClientMessagex, K interface{}), encodeValueFunc func(messageV *ClientMessagex, V interface{}))  {
		if M == nil {
			clientMessage.Add(NullFrame.Copy())
		} else {
			EntryListCodecEncode(clientMessage,M,encodeKeyFunc,encodeValueFunc)
		}
		}


func EntryListDecode(iterator *ForwardFrameIterator,
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

func EntryListCodecDecodeNullable(iterator *ForwardFrameIterator,
	decodeKeyFunc func(messageK *ForwardFrameIterator) interface{}, decodeValueFunc func(messageV *ForwardFrameIterator) interface{})  []map[interface{}]interface{} {
	if NextFrameIsNullEndFrame(iterator) {
		return nil
	}else {
		return EntryListDecode(iterator,decodeKeyFunc,decodeValueFunc)
	}

}
