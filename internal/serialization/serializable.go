package serialization

type IdentifiedDataSerializable interface {
	ReadData(input dataInput) interface{} //user implements himself
	WriteData(output dataOutput)
	GetFactoryId() int32
	GetClassId() int32
}

type IdentifiedDataSerializableFactory interface {
	create(id int32) IdentifiedDataSerializable
}
