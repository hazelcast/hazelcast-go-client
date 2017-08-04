package serialization

const UNDEFINED_POSITION = -1 // To provide optional parameter feature

type IntegerSerializer struct{}

func (i IntegerSerializer) GetId() int32 {
	return -7
}

func (i IntegerSerializer) Read(input dataInput) interface{} {
	var in interface{}
	in, _ = input.ReadInt(UNDEFINED_POSITION)
	return in
}

func (i IntegerSerializer) Write(output dataOutput, in interface{}) {
	r := in.(int32)
	output.WriteInt(r)
}
