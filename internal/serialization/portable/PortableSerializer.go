package portable

import . "github.com/hazelcast/go-client/internal/serialization"

type PortableSerializer struct {
	portableContext PortableContext
	factories       map[int32]PortableFactory
	service         SerializationService
}

func NewPortableSerializer(factories map[int32]IdentifieDataSerializableFactory) *IdentifiedDataSerializableSerializer {
	return &IdentifiedDataSerializableSerializer{factories: factories}
}

func GetId()int32{
	return -1
}

func Read()  {
	
}

func ReadObject(){

}

func Write()  {

}

func WriteObject(){

}


