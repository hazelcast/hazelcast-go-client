package bufutil

type ListMultiFrameCodec struct {

}

func ListMultiFrameCodecEncode(clientMessage ClientMessagex, T []interface{}, encodeFunction func(messagex ClientMessagex, T interface{}) )  { // , BiConsumer<ClientMessage, T>
	clientMessage.Add(BeginFrame)
	//TODO:BiConsumer olayi
	for i := 0; i < len(T) ; i++ {
		encodeFunction(clientMessage,T[i]) //f.accept //TODO
	}
	clientMessage.Add(EndFrame)
}

func ListMultiFrameCodecEncodeContainsNullable(clientMessage ClientMessagex, T []interface{}, encodeFunction func(messagex ClientMessagex, T interface{}) )  { // bu methodu verince olcak , BiConsumer<ClientMessage, T>
	clientMessage.Add(BeginFrame)
	for i := 0; i < len(T) ; i++ {
		if (T == nil){
		clientMessage.Add(NullFrame.Copy())
		}else {
			encodeFunction(clientMessage,T[i])  //f.accept //TODO
		}
	}
	clientMessage.Add(EndFrame)
}

func ListMultiFrameCodecEncodeNullable(clientMessage ClientMessagex, T []interface{}, encodeFunction func(messagex ClientMessagex, T interface{}) )  { // , BiConsumer<ClientMessage, T>
		if (T == nil){
			clientMessage.Add(NullFrame.Copy())
		}else {
			ListMultiFrameCodecEncode(clientMessage, T, encodeFunction)
		}
}

func ListMultiFrameCodecDecode(iterator ForwardFrameIterator, T []interface{}, decodeFunction func(iteratorx ForwardFrameIterator, T interface{}) )  []interface{} { // , BiConsumer<ClientMessage, T>

//TODO: result := T//(linked-list)TODO : check
//begin frame, list
iterator.Next()
for !NextFrameIsDataStructureEndFrame(iterator) {
	//TODO: result.Add(decodeFunction(iterator,T)) //TODO : how to ?
}
//end frame, list
iterator.Next()
return result
}

func ListMultiFrameCodecDecodeContainsNullable(iterator ForwardFrameIterator, T []interface{}, decodeFunction func(iteratortx ForwardFrameIterator, T interface{}) ) []interface{} { // bu methodu verince olcak , BiConsumer<ClientMessage, T>
	//TODO: result := T  //(linked-list)TODO : check
	iterator.Next()
	for !NextFrameIsDataStructureEndFrame(iterator) {
		if(NextFrameIsNullEndFrame(iterator)){
			//TODO: result.Add(nil)
		}else{
			//TODO : result.Add(decodeFunction(iterator,T))
		}
	}
	iterator.Next()
	return result
}

func ListMultiFrameCodecDecodeNullable(iterator ForwardFrameIterator, T []interface{}, decodeFunction func(iteratorx ForwardFrameIterator, T interface{}) ) interface{} { // , BiConsumer<ClientMessage, T>
		if(NextFrameIsNullEndFrame(iterator)){
			return nil
		}else{
			return ListMultiFrameCodecDecode(iterator,T,decodeFunction)
		}
}

