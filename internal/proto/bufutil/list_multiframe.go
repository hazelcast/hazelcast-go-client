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
/*

func typeCast(i interface{}) {
	switch o := i.(type) {
	case serialization.Data:
			i = o
	//case float64:
	//	fmt.Printf("%7.3f\n", o)
	//case string:
	//	fmt.Printf("%s\n", o)
	default: // covers structs and such
	//	fmt.Printf("%+v\n", o)
	}
}		*/


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


/*
func ListAddAllDecodeRequest(clientMessage *bufutil.ClientMessagex) *ListAddAllRequestParameters {
    iterator := clientMessage.FrameIterator()
    request := new(ListAddAllRequestParameters)
    //empty initial frame
    iterator.Next()
    request.name = bufutil.StringCodecDecode(iterator)
    request.valueList, _ = bufutil.ListMultiFramecode(iterator, bufutil.DataCodecDecode(iterator))
    return request
}


type decodeFunctionData func(iterator *ForwardFrameIterator) []serialization.Data


func ListMultiFramecode(iterator *ForwardFrameIterator,data interface{}) ([]serialization2.Data, error) {

	switch data.(type) {
	case decodeFunctionData:
		var result []serialization2.Data
		//begin frame, list
		iterator.Next()
		for !NextFrameIsDataStructureEndFrame(iterator) {
			result = append(result, DataCodecDecode(iterator))
		}
		//end frame, list
		iterator.Next()

		return result, nil

	}

	return nil, nil
}




 */

/*
func ListMultiFrameCodecDecode(iterator *ForwardFrameIterator, decodeFunction func(iteratorx *ForwardFrameIterator) serialization.Data {
	return DataCodecDecode(iterator)} )  []serialization.Data {
	//return func() (result []serialization.Data) {    func(x int) string { return fmt.Sprintf("%b", x) }
	result := make([]serialization.Data,0)
		//begin frame, list
		iterator.Next()
		for !NextFrameIsDataStructureEndFrame(iterator) {
			result = append(result, decodeFunction(iterator))
		}
		//end frame, list
		iterator.Next()
		return result
	//}
}	*/

/*
func decodeFunction(it *ForwardFrameIterator) i{
	switch o := i.(type) {
	case int64:
		fmt.Printf("%5d\n", o)
	case float64:
		fmt.Printf("%7.3f\n", o)
	case string:
		fmt.Printf("%s\n", o)
	default: // covers structs and such
		fmt.Printf("%+v\n", o)
	}
}
 */

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

