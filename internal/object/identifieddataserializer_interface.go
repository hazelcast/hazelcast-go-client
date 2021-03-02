package object

type IdentifiedDataSerializer interface {
	FactoryID() int32
	ClassID() int32
	WriteData(writer DataWriter)
	ReadData(reader DataReader)
}
