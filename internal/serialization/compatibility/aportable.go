package compatibility

import (
	. "github.com/hazelcast/go-client/internal/serialization/api"
)

type aPortable struct {
	bool bool
	b    byte
	c    uint16
	d    float64
	s    int16
	f    float32
	i    int32
	l    int64
	str  string
	p    Portable

	booleans  []bool
	bytes     []byte
	chars     []uint16
	doubles   []float64
	shorts    []int16
	floats    []float32
	ints      []int32
	longs     []int64
	strings   []string
	portables []Portable

	booleansNull []bool
	bytesNull    []byte
	charsNull    []uint16
	doublesNull  []float64
	shortsNull   []int16
	floatsNull   []float32
	intsNull     []int32
	longsNull    []int64
	stringsNull  []string
}

func (*aPortable) GetClassId() int32 {
	return PORTABLE_CLASS_ID
}

func (*aPortable) GetFactoryId() int32 {
	return PORTABLE_FACTORY_ID
}

func (p *aPortable) WritePortable(writer PortableWriter) {
	writer.WriteBool("bool", p.bool)
	writer.WriteByte("b", p.b)
	writer.WriteUInt16("c", p.c)
	writer.WriteFloat64("d", p.d)
	writer.WriteInt16("s", p.s)
	writer.WriteFloat32("f", p.f)
	writer.WriteInt32("i", p.i)
	writer.WriteInt64("l", p.l)
	writer.WriteUTF("str", p.str)
	if p != nil {
		writer.WritePortable("p", p)
	} else {
		writer.WriteNilPortable("p", PORTABLE_FACTORY_ID, PORTABLE_CLASS_ID)
	}
	writer.WriteBoolArray("booleans", p.booleans)
	writer.WriteByteArray("bs", p.bytes)
	writer.WriteUInt16Array("cs", p.chars)
	writer.WriteFloat64Array("ds", p.doubles)
	writer.WriteInt16Array("ss", p.shorts)
	writer.WriteFloat32Array("fs", p.floats)
	writer.WriteInt32Array("is", p.ints)
	writer.WriteInt64Array("ls", p.longs)
	writer.WriteUTFArray("strs", p.strings)
	writer.WritePortableArray("ps", p.portables)

	writer.WriteBoolArray("booleansNull", p.booleansNull)
	writer.WriteByteArray("bsNull", p.bytesNull)
	writer.WriteUInt16Array("csNull", p.charsNull)
	writer.WriteFloat64Array("dsNull", p.doublesNull)
	writer.WriteInt16Array("ssNull", p.shortsNull)
	writer.WriteFloat32Array("fsNull", p.floatsNull)
	writer.WriteInt32Array("isNull", p.intsNull)
	writer.WriteInt64Array("lsNull", p.longsNull)
	writer.WriteUTFArray("strsNull", p.stringsNull)

	//ObjectDataOutput dataOutput = writer.GetRawDataOutput()

}

func (p *aPortable) ReadPortable(reader PortableReader) {
	p.bool, _ = reader.ReadBool("bool")
	p.b, _ = reader.ReadByte("b")
	p.c, _ = reader.ReadUInt16("c")
	p.d, _ = reader.ReadFloat64("d")
	p.s, _ = reader.ReadInt16("s")
	p.f, _ = reader.ReadFloat32("f")
	p.i, _ = reader.ReadInt32("i")
	p.l, _ = reader.ReadInt64("l")
	p.str, _ = reader.ReadUTF("str")
	p.p, _ = reader.ReadPortable("p")

	p.booleans, _ = reader.ReadBoolArray("booleans")
	p.bytes, _ = reader.ReadByteArray("bs")
	p.chars, _ = reader.ReadUInt16Array("cs")
	p.doubles, _ = reader.ReadFloat64Array("ds")
	p.shorts, _ = reader.ReadInt16Array("ss")
	p.floats, _ = reader.ReadFloat32Array("fs")
	p.ints, _ = reader.ReadInt32Array("is")
	p.longs, _ = reader.ReadInt64Array("ls")
	p.strings, _ = reader.ReadUTFArray("strs")
	p.portables, _ = reader.ReadPortableArray("ps")

	p.booleansNull, _ = reader.ReadBoolArray("booleansNull")
	p.bytesNull, _ = reader.ReadByteArray("bsNull")
	p.charsNull, _ = reader.ReadUInt16Array("csNull")
	p.doublesNull, _ = reader.ReadFloat64Array("dsNull")
	p.shortsNull, _ = reader.ReadInt16Array("ssNull")
	p.floatsNull, _ = reader.ReadFloat32Array("fsNull")
	p.intsNull, _ = reader.ReadInt32Array("isNull")
	p.longsNull, _ = reader.ReadInt64Array("lsNull")
	p.stringsNull, _ = reader.ReadUTFArray("strsNull")
}
