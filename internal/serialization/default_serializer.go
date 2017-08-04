package serialization

type IntegerSerializer struct{}

func (i IntegerSerializer) GetId() int32 {
	return -7
}

func (i IntegerSerializer) Read(input dataInput) interface{} {
	var in interface{}
	in, _ = input.ReadInt32()
	return in
}

func (i IntegerSerializer) Write(output dataOutput, in interface{}) {
	r := in.(int32)
	output.WriteInt32(r)
}

type DoubleSerializer struct{}

func (i DoubleSerializer) GetId() int32 {
	return -7
}

func (i DoubleSerializer) Read(input dataInput) interface{} {
	var in interface{}
	in, _ = input.ReadFloat64()
	return in
}

func (i DoubleSerializer) Write(output dataOutput, in interface{}) {
	r := in.(float64)
	output.WriteFloat64(r)
}

