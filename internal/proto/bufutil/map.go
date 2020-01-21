package bufutil

type MapCodec struct {

}

func MapCodecEncode(clientMessage *ClientMessage, M map[interface{}]interface{},
encodeKeyFunc func(messageK *ClientMessage, K interface{}), encodeValueFunc func(messageV *ClientMessage, V interface{}) )  {
	clientMessage.Add(BeginFrame.Copy())
		for k, v := range M{
	encodeKeyFunc(clientMessage, k)
	encodeValueFunc(clientMessage, v)
	}
	clientMessage.Add(EndFrame.Copy())
}

func MapCodecEncodeNullable(clientMessage *ClientMessage, M map[interface{}]interface{},
	encodeKeyFunc func(messageK *ClientMessage, K interface{}), encodeValueFunc func(messageV *ClientMessage, V interface{}))  {
	if M == nil {
		clientMessage.Add(NullFrame.Copy())
	} else {
		MapCodecEncode(clientMessage,M,encodeKeyFunc,encodeValueFunc)
	}
}


func MapCodecDecode(iterator *ForwardFrameIterator,
	decodeKeyFunc func(messageK *ForwardFrameIterator) interface{}, decodeValueFunc func(messageV *ForwardFrameIterator) interface{}) map[interface{}]interface{}  {
	var result = make(map[interface{}]interface{})
	//begin frame, map
	iterator.Next()
	for !NextFrameIsDataStructureEndFrame(iterator) {
		key := decodeKeyFunc(iterator)
		value := decodeValueFunc(iterator)
		result[key]=value
	}

	return result
}

func MapCodecDecodeNullable(iterator *ForwardFrameIterator,
	decodeKeyFunc func(messageK *ForwardFrameIterator) interface{}, decodeValueFunc func(messageV *ForwardFrameIterator) interface{}) map[interface{}]interface{} {
	if !NextFrameIsNullEndFrame(iterator) {
			return MapCodecDecode(iterator, decodeKeyFunc,decodeValueFunc)
		} else {
			return nil
	}
}

