package hazelcast

type ClientConfig struct {
}

type SerializationConfig struct{
	IsBigEndian bool
	// dataSerializableFactories map[int32]IdentifiedDataSerializableFactory
	// portableFactories map[int32]
	portableVersion int32
	//customSerializers []
	//globalSerializer
}

func NewSerializationConfig() SerializationConfig{
	return SerializationConfig{IsBigEndian:true,portableVersion:0}
}

func NewClientConfig() ClientConfig {
	return ClientConfig{}
}
