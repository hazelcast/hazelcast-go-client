package bufutil

type CodecUtil struct {

}
//TODO: check
func FastForwardToEndFrame(iterator *ForwardFrameIterator){
	// We are starting from 1 because of the BEGIN_FRAME we read
	// in the beginning of the decode method
	numberOfExpectedEndFrames := 1
	var frame *Frame
	for numberOfExpectedEndFrames != 0 {
		frame = iterator.Next()
		if frame.IsEndFrame() {
			numberOfExpectedEndFrames--
		} else if frame.IsBeginFrame() {
			numberOfExpectedEndFrames++
		}
	}
}

func EncodeNullable(clientMessage *ClientMessage, T interface{}, encodeFunction func(messagex *ClientMessage, T interface{})) {
	if T == nil {
		clientMessage.Add(NullFrame.Copy())
	} else {
		encodeFunction(clientMessage, T)
	}
}

func NextFrameIsDataStructureEndFrame(iterator *ForwardFrameIterator) bool {
	return iterator.PeekNext().IsEndFrame()
}

func NextFrameIsNullEndFrame(iterator *ForwardFrameIterator) bool {
	isNull := iterator.PeekNext().IsNullFrame()
	if isNull {
		iterator.Next()
	}
	return isNull
}

func DecodeNullable(iterator *ForwardFrameIterator, decodeFunction interface{} ) interface{} {
		if NextFrameIsDataStructureEndFrame(iterator) == false {
			return decodeFunction //TODO: return check
		}else {
			return nil
		}
}




/*
func DecodeNullable(iterator *ForwardFrameIterator, decodeFunction func(itFrame *ForwardFrameIterator) interface{} ) interface{} {
	if NextFrameIsDataStructureEndFrame(iterator) == false {
		return decodeFunction(iterator)
	}else {
		return nil
	}
}func DecodeNull(iterator *ForwardFrameIterator,data interface{}) (serialization.Data, error) {

	switch data.(type) {
	case serialization.Data:
		if NextFrameIsDataStructureEndFrame(iterator) == false {
			return DataCodecDecode(iterator), nil //TODO: return check
		}else {
			return nil,nil
		}

	}
	return nil,nil
}
 */