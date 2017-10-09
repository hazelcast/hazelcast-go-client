package compatibility

import "github.com/hazelcast/go-client/internal/serialization/api"

type AnInnerPortable struct {
	anInt  int32
	aFloat float32
}

func (*AnInnerPortable) FactoryId() int32 {
	return PORTABLE_FACTORY_ID
}

func (*AnInnerPortable) ClassId() int32 {
	return INNER_PORTABLE_CLASS_ID
}

func (ip *AnInnerPortable) WritePortable(writer api.PortableWriter) {
	writer.WriteInt32("i", ip.anInt)
	writer.WriteFloat32("f", ip.aFloat)
}

func (ip *AnInnerPortable) ReadPortable(reader api.PortableReader) {
	ip.anInt, _ = reader.ReadInt32("i")
	ip.aFloat, _ = reader.ReadFloat32("f")
}
