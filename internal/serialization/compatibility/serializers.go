package compatibility

import (
	pacbytes "bytes"
	"encoding/binary"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
	"math"
)

type CustomStreamSerializable struct {
	I int32
	F float32
}

type CustomStreamSerializer struct{}

func (e CustomStreamSerializer) ID() (id int32) {
	return CustomStreamSerializableId
}
func (e CustomStreamSerializer) Read(input serialization.DataInput) interface{} {
	i := input.ReadInt32()
	f := input.ReadFloat32()
	return CustomStreamSerializable{I: i, F: f}
}

func (e CustomStreamSerializer) Write(out serialization.DataOutput, object interface{}) {
	css, ok := object.(CustomStreamSerializable)
	if !ok {
		panic("can serialize only CustomStreamSerializable")
	}
	out.WriteInt32(css.I)
	out.WriteFloat32(css.F)
}

type CustomByteArraySerializable struct {
	I int32
	F float32
}

type CustomByteArraySerializer struct {
}

func (e CustomByteArraySerializer) ID() (id int32) {
	return CustomByteArraySerializableId
}
func (e CustomByteArraySerializer) Read(input serialization.DataInput) interface{} {
	buf := pacbytes.NewBuffer(input.ReadByteArray())
	var i int32
	var f float32
	err := binary.Read(buf, binary.BigEndian, &i)
	if err != nil {
		return nil
	}
	err = binary.Read(buf, binary.BigEndian, &f)
	if err != nil {
		return nil
	}
	return CustomByteArraySerializable{I: i, F: f}
}

func (e CustomByteArraySerializer) Write(output serialization.DataOutput, object interface{}) {
	cba, ok := object.(CustomByteArraySerializable)
	if !ok {
		panic("can serialize only CustomByteArraySerializable")
	}
	buf := make([]byte, 10)
	binary.BigEndian.PutUint32(buf, uint32(cba.I))
	binary.BigEndian.PutUint32(buf[4:], math.Float32bits(cba.F))
	output.WriteByteArray(buf)
}

type AnIdentifiedDataSerializable struct {
	bool bool
	b    byte
	c    uint16
	d    float64
	s    int16
	f    float32
	i    int32
	l    int64
	str  string

	booleans []bool
	bytes    []byte
	chars    []uint16
	doubles  []float64
	shorts   []int16
	floats   []float32
	ints     []int32
	longs    []int64
	strings  []string

	booleansNil []bool
	bytesNil    []byte
	charsNil    []uint16
	doublesNil  []float64
	shortsNil   []int16
	floatsNil   []float32
	intsNil     []int32
	longsNil    []int64
	stringsNil  []string

	byteSize      byte
	bytesFully    []byte
	bytesOffset   []byte
	strChars      []uint16
	strBytes      []byte
	unsignedByte  uint8
	unsignedShort uint16

	portableObject                    *AnInnerPortable
	identifiedDataSerializableObject  serialization.IdentifiedDataSerializable
	customStreamSerializableObject    CustomStreamSerializable
	customByteArraySerializableObject CustomByteArraySerializable

	data iserialization.Data
}

func (i AnIdentifiedDataSerializable) FactoryID() int32 {
	return IdentifiedDataSerializableFactoryId
}
func (i AnIdentifiedDataSerializable) ClassID() int32 {
	return IdentifiedDataSerializableClassId
}

func (i AnIdentifiedDataSerializable) WriteData(output serialization.DataOutput) {
	output.WriteBool(i.bool)
	output.WriteByte(i.b)
	output.WriteUInt16(i.c)
	output.WriteFloat64(i.d)
	output.WriteInt16(i.s)
	output.WriteFloat32(i.f)
	output.WriteInt32(i.i)
	output.WriteInt64(i.l)
	output.WriteString(i.str)

	output.WriteBoolArray(i.booleans)
	output.WriteByteArray(i.bytes)
	output.WriteUInt16Array(i.chars)
	output.WriteFloat64Array(i.doubles)
	output.WriteInt16Array(i.shorts)
	output.WriteFloat32Array(i.floats)
	output.WriteInt32Array(i.ints)
	output.WriteInt64Array(i.longs)
	output.WriteStringArray(i.strings)

	output.WriteBoolArray(i.booleansNil)
	output.WriteByteArray(i.bytesNil)
	output.WriteUInt16Array(i.charsNil)
	output.WriteFloat64Array(i.doublesNil)
	output.WriteInt16Array(i.shortsNil)
	output.WriteFloat32Array(i.floatsNil)
	output.WriteInt32Array(i.intsNil)
	output.WriteInt64Array(i.longsNil)
	output.WriteStringArray(i.stringsNil)

	i.byteSize = byte(len(i.bytes))
	output.WriteByte(i.byteSize)
	writeRawBytes(output, i.bytes)
	output.WriteByte(i.bytes[1])
	output.WriteByte(i.bytes[2])
	output.WriteInt32(int32(len(i.str)))
	for _, r := range i.str {
		output.WriteUInt16(uint16(r))
	}
	output.WriteStringBytes(i.str)
	output.WriteByte(i.unsignedByte)
	output.WriteUInt16(i.unsignedShort)

	output.WriteObject(i.portableObject)
	output.WriteObject(i.identifiedDataSerializableObject)
	output.WriteObject(i.customByteArraySerializableObject)
	output.WriteObject(i.customStreamSerializableObject)

	writeDataToOutput(output, i.data)
}

func (i *AnIdentifiedDataSerializable) ReadData(input serialization.DataInput) {
	i.bool = input.ReadBool()
	i.b = input.ReadByte()
	i.c = input.ReadUInt16()
	i.d = input.ReadFloat64()
	i.s = input.ReadInt16()
	i.f = input.ReadFloat32()
	i.i = input.ReadInt32()
	i.l = input.ReadInt64()
	i.str = input.ReadString()

	i.booleans = input.ReadBoolArray()
	i.bytes = input.ReadByteArray()
	i.chars = input.ReadUInt16Array()
	i.doubles = input.ReadFloat64Array()
	i.shorts = input.ReadInt16Array()
	i.floats = input.ReadFloat32Array()
	i.ints = input.ReadInt32Array()
	i.longs = input.ReadInt64Array()
	i.strings = input.ReadStringArray()

	i.booleansNil = input.ReadBoolArray()
	i.bytesNil = input.ReadByteArray()
	i.charsNil = input.ReadUInt16Array()
	i.doublesNil = input.ReadFloat64Array()
	i.shortsNil = input.ReadInt16Array()
	i.floatsNil = input.ReadFloat32Array()
	i.intsNil = input.ReadInt32Array()
	i.longsNil = input.ReadInt64Array()
	i.stringsNil = input.ReadStringArray()

	i.byteSize = input.ReadByte()
	i.bytesFully = readRawBytes(input, int(i.byteSize))
	i.bytesOffset = readRawBytes(input, 2)

	strSize := input.ReadInt32()
	i.strChars = make([]uint16, strSize)
	for j := 0; j < int(strSize); j++ {
		i.strChars[j] = input.ReadUInt16()
	}
	i.strBytes = readRawBytes(input, int(strSize))
	i.unsignedByte = input.ReadByte()
	i.unsignedShort = input.ReadUInt16()

	portableObject := input.ReadObject()
	if portableObject != nil {
		i.portableObject = portableObject.(*AnInnerPortable)
	}
	identifiedDataSerializableObject := input.ReadObject()
	if identifiedDataSerializableObject != nil {
		i.identifiedDataSerializableObject = identifiedDataSerializableObject.(serialization.IdentifiedDataSerializable)
	}
	customByteArraySerializableObject := input.ReadObject()
	if customByteArraySerializableObject != nil {
		i.customByteArraySerializableObject = customByteArraySerializableObject.(CustomByteArraySerializable)
	}
	customStreamSerializableObject := input.ReadObject()
	if customStreamSerializableObject != nil {
		i.customStreamSerializableObject = customStreamSerializableObject.(CustomStreamSerializable)
	}
	i.data = readDataFromInput(input)
}

func readDataFromInput(inp serialization.DataInput) iserialization.Data {
	buff := inp.ReadByteArray()
	if buff != nil {
		return buff
	}
	return nil
}

func writeDataToOutput(out serialization.DataOutput, data iserialization.Data) {
	var payload []byte
	if data.Type() != iserialization.TypeNil {
		payload = data.ToByteArray()
	} else {
		payload = nil
	}
	out.WriteByteArray(payload)
}

type IdentifiedFactory struct{}

func (f IdentifiedFactory) FactoryID() int32 {
	return IdentifiedDataSerializableFactoryId
}

func (f IdentifiedFactory) Create(classID int32) serialization.IdentifiedDataSerializable {
	if classID == IdentifiedDataSerializableClassId {
		return &AnIdentifiedDataSerializable{}
	}
	return nil
}

type AnInnerPortable struct {
	i int32
	f float32
}

func (p AnInnerPortable) FactoryID() int32 {
	return PortableFactoryId
}

func (p AnInnerPortable) ClassID() int32 {
	return InnerPortableClassId
}

func (p AnInnerPortable) WritePortable(out serialization.PortableWriter) {
	out.WriteInt32("i", p.i)
	out.WriteFloat32("f", p.f)
}

func (p *AnInnerPortable) ReadPortable(reader serialization.PortableReader) {
	p.i = reader.ReadInt32("i")
	p.f = reader.ReadFloat32("f")
}

type APortable struct {
	boolean         bool
	b               byte
	c               uint16
	d               float64
	s               int16
	f               float32
	i               int32
	l               int64
	str             string
	bd              types.Decimal
	ld              types.LocalDate
	lt              types.LocalTime
	ldt             types.LocalDateTime
	odt             types.OffsetDateTime
	p               serialization.Portable
	booleans        []bool
	bytes           []byte
	chars           []uint16
	doubles         []float64
	shorts          []int16
	floats          []float32
	ints            []int32
	longs           []int64
	strings         []string
	decimals        []types.Decimal
	dates           []types.LocalDate
	times           []types.LocalTime
	dateTimes       []types.LocalDateTime
	offsetDateTimes []types.OffsetDateTime
	portables       []serialization.Portable

	booleansNil []bool
	bytesNil    []byte
	charsNil    []uint16
	doublesNil  []float64
	shortsNil   []int16
	floatsNil   []float32
	intsNil     []int32
	longsNil    []int64
	stringsNil  []string

	byteSize      byte
	bytesFully    []byte
	bytesOffset   []byte
	strChars      []uint16
	strBytes      []byte
	unsignedByte  uint8
	unsignedShort uint16

	portableObject                    serialization.Portable
	identifiedDataSerializableObject  *AnIdentifiedDataSerializable
	customStreamSerializableObject    CustomStreamSerializable
	customByteArraySerializableObject CustomByteArraySerializable
	data                              iserialization.Data
}

func (p APortable) FactoryID() int32 {
	return PortableFactoryId
}

func (p APortable) ClassID() int32 {
	return PortableClassId
}

func (p APortable) WritePortable(writer serialization.PortableWriter) {
	writer.WriteBool("bool", p.boolean)
	writer.WriteByte("b", p.b)
	writer.WriteUInt16("c", p.c)
	writer.WriteFloat64("d", p.d)
	writer.WriteInt16("s", p.s)
	writer.WriteFloat32("f", p.f)
	writer.WriteInt32("i", p.i)
	writer.WriteInt64("l", p.l)
	writer.WriteString("str", p.str)
	writer.WriteDecimal("bd", &p.bd)
	writer.WriteDate("ld", &p.ld)
	writer.WriteTime("lt", &p.lt)
	writer.WriteTimestamp("ldt", &p.ldt)
	writer.WriteTimestampWithTimezone("odt", &p.odt)
	if p.p != nil {
		writer.WritePortable("p", p.p)
	} else {
		writer.WriteNilPortable("p", PortableFactoryId, PortableClassId)
	}
	writer.WriteBoolArray("booleans", p.booleans)
	writer.WriteByteArray("bs", p.bytes)
	writer.WriteUInt16Array("cs", p.chars)
	writer.WriteFloat64Array("ds", p.doubles)
	writer.WriteInt16Array("ss", p.shorts)
	writer.WriteFloat32Array("fs", p.floats)
	writer.WriteInt32Array("is", p.ints)
	writer.WriteInt64Array("ls", p.longs)
	writer.WriteStringArray("strs", p.strings)
	writer.WriteDecimalArray("decimals", p.decimals)
	writer.WriteDateArray("dates", p.dates)
	writer.WriteTimeArray("times", p.times)
	writer.WriteTimestampArray("dateTimes", p.dateTimes)
	writer.WriteTimestampWithTimezoneArray("offsetDateTimes", p.offsetDateTimes)
	writer.WritePortableArray("ps", p.portables)

	writer.WriteBoolArray("booleansNull", p.booleansNil)
	writer.WriteByteArray("bsNull", p.bytesNil)
	writer.WriteUInt16Array("csNull", p.charsNil)
	writer.WriteFloat64Array("dsNull", p.doublesNil)
	writer.WriteInt16Array("ssNull", p.shortsNil)
	writer.WriteFloat32Array("fsNull", p.floatsNil)
	writer.WriteInt32Array("isNull", p.intsNil)
	writer.WriteInt64Array("lsNull", p.longsNil)
	writer.WriteStringArray("strsNull", p.stringsNil)

	out := writer.GetRawDataOutput()

	out.WriteBool(p.boolean)
	out.WriteByte(p.b)
	out.WriteUInt16(p.c)
	out.WriteFloat64(p.d)
	out.WriteInt16(p.s)
	out.WriteFloat32(p.f)
	out.WriteInt32(p.i)
	out.WriteInt64(p.l)
	out.WriteString(p.str)

	out.WriteBoolArray(p.booleans)
	out.WriteByteArray(p.bytes)
	out.WriteUInt16Array(p.chars)
	out.WriteFloat64Array(p.doubles)
	out.WriteInt16Array(p.shorts)
	out.WriteFloat32Array(p.floats)
	out.WriteInt32Array(p.ints)
	out.WriteInt64Array(p.longs)
	out.WriteStringArray(p.strings)

	out.WriteBoolArray(p.booleansNil)
	out.WriteByteArray(p.bytesNil)
	out.WriteUInt16Array(p.charsNil)
	out.WriteFloat64Array(p.doublesNil)
	out.WriteInt16Array(p.shortsNil)
	out.WriteFloat32Array(p.floatsNil)
	out.WriteInt32Array(p.intsNil)
	out.WriteInt64Array(p.longsNil)
	out.WriteStringArray(p.stringsNil)

	byteSize := byte(len(p.bytes))
	out.WriteByte(byteSize)
	writeRawBytes(out, p.bytes)
	out.WriteByte(p.bytes[1])
	out.WriteByte(p.bytes[2])
	out.WriteInt32(int32(len(p.str)))
	for _, r := range p.str {
		out.WriteUInt16(uint16(r))
	}
	out.WriteStringBytes(p.str)
	out.WriteByte(p.unsignedByte)
	out.WriteUInt16(p.unsignedShort)

	out.WriteObject(p.portableObject)
	out.WriteObject(p.identifiedDataSerializableObject)
	out.WriteObject(p.customByteArraySerializableObject)
	out.WriteObject(p.customStreamSerializableObject)

	writeDataToOutput(out, p.data)
}

func (p *APortable) ReadPortable(reader serialization.PortableReader) {
	p.boolean = reader.ReadBool("bool")
	p.b = reader.ReadByte("b")
	p.c = reader.ReadUInt16("c")
	p.d = reader.ReadFloat64("d")
	p.s = reader.ReadInt16("s")
	p.f = reader.ReadFloat32("f")
	p.i = reader.ReadInt32("i")
	p.l = reader.ReadInt64("l")
	p.str = reader.ReadString("str")
	p.bd = *reader.ReadDecimal("bd")
	p.ld = *reader.ReadDate("ld")
	p.lt = *reader.ReadTime("lt")
	p.ldt = *reader.ReadTimestamp("ldt")
	p.odt = *reader.ReadTimestampWithTimezone("odt")
	p.p = reader.ReadPortable("p")

	p.booleans = reader.ReadBoolArray("booleans")
	p.bytes = reader.ReadByteArray("bs")
	p.chars = reader.ReadUInt16Array("cs")
	p.doubles = reader.ReadFloat64Array("ds")
	p.shorts = reader.ReadInt16Array("ss")
	p.floats = reader.ReadFloat32Array("fs")
	p.ints = reader.ReadInt32Array("is")
	p.longs = reader.ReadInt64Array("ls")
	p.strings = reader.ReadStringArray("strs")
	p.decimals = reader.ReadDecimalArray("decimals")
	p.dates = reader.ReadDateArray("dates")
	p.times = reader.ReadTimeArray("times")
	p.dateTimes = reader.ReadTimestampArray("dateTimes")
	p.offsetDateTimes = reader.ReadTimestampWithTimezoneArray("offsetDateTimes")
	p.portables = reader.ReadPortableArray("ps")

	p.booleansNil = reader.ReadBoolArray("booleansNull")
	p.bytesNil = reader.ReadByteArray("bsNull")
	p.charsNil = reader.ReadUInt16Array("csNull")
	p.doublesNil = reader.ReadFloat64Array("dsNull")
	p.shortsNil = reader.ReadInt16Array("ssNull")
	p.floatsNil = reader.ReadFloat32Array("fsNull")
	p.intsNil = reader.ReadInt32Array("isNull")
	p.longsNil = reader.ReadInt64Array("lsNull")
	p.stringsNil = reader.ReadStringArray("strsNull")

	dataInput := reader.GetRawDataInput()

	p.boolean = dataInput.ReadBool()
	p.b = dataInput.ReadByte()
	p.c = dataInput.ReadUInt16()
	p.d = dataInput.ReadFloat64()
	p.s = dataInput.ReadInt16()
	p.f = dataInput.ReadFloat32()
	p.i = dataInput.ReadInt32()
	p.l = dataInput.ReadInt64()
	p.str = dataInput.ReadString()

	p.booleans = dataInput.ReadBoolArray()
	p.bytes = dataInput.ReadByteArray()
	p.chars = dataInput.ReadUInt16Array()
	p.doubles = dataInput.ReadFloat64Array()
	p.shorts = dataInput.ReadInt16Array()
	p.floats = dataInput.ReadFloat32Array()
	p.ints = dataInput.ReadInt32Array()
	p.longs = dataInput.ReadInt64Array()
	p.strings = dataInput.ReadStringArray()

	p.booleansNil = dataInput.ReadBoolArray()
	p.bytesNil = dataInput.ReadByteArray()
	p.charsNil = dataInput.ReadUInt16Array()
	p.doublesNil = dataInput.ReadFloat64Array()
	p.shortsNil = dataInput.ReadInt16Array()
	p.floatsNil = dataInput.ReadFloat32Array()
	p.intsNil = dataInput.ReadInt32Array()
	p.longsNil = dataInput.ReadInt64Array()
	p.stringsNil = dataInput.ReadStringArray()

	p.byteSize = dataInput.ReadByte()
	p.bytesFully = readRawBytes(dataInput, int(p.byteSize))
	p.bytesOffset = readRawBytes(dataInput, 2)

	strSize := dataInput.ReadInt32()
	p.strChars = make([]uint16, strSize)
	for j := 0; j < int(strSize); j++ {
		p.strChars[j] = dataInput.ReadUInt16()
	}
	p.strBytes = readRawBytes(dataInput, int(strSize))
	p.unsignedByte = dataInput.ReadByte()
	p.unsignedShort = dataInput.ReadUInt16()

	portableObject := dataInput.ReadObject()
	if portableObject != nil {
		p.portableObject = portableObject.(*AnInnerPortable)
	}
	identifiedDataSerializableObject := dataInput.ReadObject()
	if identifiedDataSerializableObject != nil {
		p.identifiedDataSerializableObject = identifiedDataSerializableObject.(*AnIdentifiedDataSerializable)
	}
	customByteArraySerializableObject := dataInput.ReadObject()
	if customByteArraySerializableObject != nil {
		p.customByteArraySerializableObject = customByteArraySerializableObject.(CustomByteArraySerializable)
	}
	customStreamSerializableObject := dataInput.ReadObject()
	if customStreamSerializableObject != nil {
		p.customStreamSerializableObject = customStreamSerializableObject.(CustomStreamSerializable)
	}
	p.data = readDataFromInput(dataInput)
}

type PortableFactory struct{}

func (p PortableFactory) FactoryID() int32 {
	return PortableFactoryId
}

func (p PortableFactory) Create(classID int32) serialization.Portable {
	if classID == InnerPortableClassId {
		return &AnInnerPortable{}
	} else if classID == PortableClassId {
		return &APortable{}
	}
	return nil
}
