package bufutil

type ListMultiFrameCodec struct {

}

func ListMultiFrameCodecEncode(clientMessage *ClientMessagex, T []interface{}, encodeFunction func(messagex *ClientMessagex, T interface{}) )  {
	clientMessage.Add(BeginFrame) //TODO: clientMessage *
	for i := 0; i < len(T) ; i++ {
		encodeFunction(clientMessage,T[i])
	}
	clientMessage.Add(EndFrame)
}

func ListMultiFrameCodecEncodeContainsNullable(clientMessage ClientMessagex, T []interface{}, encodeFunction func(messagex ClientMessagex, T interface{}) )  {
	clientMessage.Add(BeginFrame)
	for i := 0; i < len(T) ; i++ {
		if T == nil{
		clientMessage.Add(NullFrame.Copy())
		}else {
			encodeFunction(clientMessage,T[i])  //f.accept //TODO
		}
	}
	clientMessage.Add(EndFrame)
}

func ListMultiFrameCodecEncodeNullable(clientMessage *ClientMessagex, T []interface{}, encodeFunction func(messagex *ClientMessagex, T interface{}) )  { // , BiConsumer<ClientMessage, T>
		if T == nil{
			clientMessage.Add(NullFrame.Copy())
		}else {
			ListMultiFrameCodecEncode(clientMessage, T, encodeFunction)
		}
}

func ListMultiFrameCodecDecode(iterator *ForwardFrameIterator, decodeFunction func(iteratorx *ForwardFrameIterator) interface{} )  []interface{} {
	var result []interface{}
	//begin frame, list
	iterator.Next()
	for !NextFrameIsDataStructureEndFrame(iterator) {
		result = append(result, decodeFunction(iterator))
	}
	//end frame, list
	iterator.Next()
	return result
}

func ListMultiFrameCodecDecodeContainsNullable(iterator *ForwardFrameIterator, decodeFunction func(iteratortx *ForwardFrameIterator) interface{} ) []interface{} {
	var result []interface{}
	iterator.Next()
	for !NextFrameIsDataStructureEndFrame(iterator) {
		if NextFrameIsNullEndFrame(iterator) {
			result = append(result, nil)
		}else{
			result = append(result, decodeFunction(iterator))
		}
	}
	iterator.Next()
	return result
}

func ListMultiFrameCodecDecodeNullable(iterator *ForwardFrameIterator, decodeNullableFunction func(iteratorx *ForwardFrameIterator) interface{}) []interface{} {
	if NextFrameIsNullEndFrame(iterator) {
		return nil
	}else{ //TODO
		return ListMultiFrameCodecDecode(iterator, decodeNullableFunction)
	}
}

