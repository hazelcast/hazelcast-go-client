package compatibility

import (
	pacbytes "bytes"
	"encoding/binary"
	pactypes "go/types"
	"math"
	"math/big"
	"time"

	"github.com/hazelcast/hazelcast-go-client/aggregate"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	identifiedDataSerializableFactoryId = 1
	portableFactoryId                   = 1
	portableClassId                     = 1
	innerPortableClassId                = 2
	identifiedDataSerializableClassId   = 1
	customStreamSerializableId          = 1
	customByteArraySerializableId       = 2
)

type CustomStreamSerializable struct {
	I int32
	F float32
}

type CustomStreamSerializer struct{}

func (e CustomStreamSerializer) ID() (id int32) {
	return customStreamSerializableId
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

type CustomByteArraySerializer struct{}

func (e CustomByteArraySerializer) ID() (id int32) {
	return customByteArraySerializableId
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
	identifiedDataSerializableObject  serialization.IdentifiedDataSerializable
	portableObject                    *AnInnerPortable
	str                               string
	booleansNil                       []bool
	bytesFully                        []byte
	bytesNil                          []byte
	charsNil                          []uint16
	data                              iserialization.Data
	strBytes                          []byte
	booleans                          []bool
	bytes                             []byte
	chars                             []uint16
	doubles                           []float64
	shorts                            []int16
	floats                            []float32
	ints                              []int32
	longs                             []int64
	strings                           []string
	strChars                          []uint16
	bytesOffset                       []byte
	doublesNil                        []float64
	stringsNil                        []string
	shortsNil                         []int16
	floatsNil                         []float32
	intsNil                           []int32
	longsNil                          []int64
	d                                 float64
	l                                 int64
	customByteArraySerializableObject CustomByteArraySerializable
	customStreamSerializableObject    CustomStreamSerializable
	f                                 float32
	i                                 int32
	s                                 int16
	c                                 uint16
	unsignedShort                     uint16
	bool                              bool
	b                                 byte
	unsignedByte                      uint8
	byteSize                          byte
}

func (i AnIdentifiedDataSerializable) FactoryID() int32 {
	return identifiedDataSerializableFactoryId
}

func (i AnIdentifiedDataSerializable) ClassID() int32 {
	return identifiedDataSerializableClassId
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
	return identifiedDataSerializableFactoryId
}

func (f IdentifiedFactory) Create(classID int32) serialization.IdentifiedDataSerializable {
	if classID == identifiedDataSerializableClassId {
		return &AnIdentifiedDataSerializable{}
	}
	return nil
}

type AnInnerPortable struct {
	i int32
	f float32
}

func (p AnInnerPortable) FactoryID() int32 {
	return portableFactoryId
}

func (p AnInnerPortable) ClassID() int32 {
	return innerPortableClassId
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
	ld                                types.LocalDate
	lt                                types.LocalTime
	ldt                               types.LocalDateTime
	odt                               types.OffsetDateTime
	p                                 serialization.Portable
	portableObject                    serialization.Portable
	identifiedDataSerializableObject  *AnIdentifiedDataSerializable
	str                               string
	bd                                types.Decimal
	portables                         []serialization.Portable
	floatsNil                         []float32
	bytes                             []byte
	chars                             []uint16
	doubles                           []float64
	shorts                            []int16
	floats                            []float32
	ints                              []int32
	longs                             []int64
	strings                           []string
	decimals                          []types.Decimal
	dates                             []types.LocalDate
	times                             []types.LocalTime
	dateTimes                         []types.LocalDateTime
	offsetDateTimes                   []types.OffsetDateTime
	data                              iserialization.Data
	booleansNil                       []bool
	bytesNil                          []byte
	charsNil                          []uint16
	doublesNil                        []float64
	shortsNil                         []int16
	booleans                          []bool
	intsNil                           []int32
	longsNil                          []int64
	stringsNil                        []string
	strBytes                          []byte
	strChars                          []uint16
	bytesOffset                       []byte
	bytesFully                        []byte
	d                                 float64
	l                                 int64
	customByteArraySerializableObject CustomByteArraySerializable
	customStreamSerializableObject    CustomStreamSerializable
	f                                 float32
	i                                 int32
	s                                 int16
	unsignedShort                     uint16
	c                                 uint16
	unsignedByte                      uint8
	b                                 byte
	boolean                           bool
	byteSize                          byte
}

func (p APortable) FactoryID() int32 {
	return portableFactoryId
}

func (p APortable) ClassID() int32 {
	return portableClassId
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
		writer.WriteNilPortable("p", portableFactoryId, portableClassId)
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
	return portableFactoryId
}

func (p PortableFactory) Create(classID int32) serialization.Portable {
	if classID == innerPortableClassId {
		return &AnInnerPortable{}
	}
	if classID == portableClassId {
		return &APortable{}
	}
	return nil
}

func writeRawBytes(o serialization.DataOutput, b []byte) {
	for _, i := range b {
		o.WriteByte(i)
	}
}

func readRawBytes(i serialization.DataInput, size int) []byte {
	b := make([]byte, size)
	for idx := range b {
		b[idx] = i.ReadByte()
	}
	return b
}

func makeTestObjects() map[string]interface{} {
	var (
		allTestObjects map[string]interface{} = nil
		aNullObject    pactypes.Object        = nil
		aBoolean       bool                   = true
		aByte          byte                   = 113
		aChar          uint16                 = 'x'
		aDouble        float64                = -897543.3678909
		aShort         int16                  = -500
		aFloat         float32                = 900.5678
		anInt          int32                  = 56789
		aLong          int64                  = -50992225
		anSqlString    string                 = "this > 5 AND this < 100"
		aUUID          types.UUID             = types.NewUUIDWith(uint64(aLong), uint64(anInt))

		booleans = []bool{true, false, true}
		// byte is signed in Java but unsigned in Go!
		bytes   = []byte{112, 4, 1, 4, 112, 35, 43}
		chars   = []uint16{'a', 'b', 'c'}
		doubles = []float64{-897543.3678909, 11.1, 22.2, 33.3}
		shorts  = []int16{-500, 2, 3}
		floats  = []float32{900.5678, 1.0, 2.1, 3.4}
		ints    = []int32{56789, 2, 3}
		longs   = []int64{-50992225, 1231232141, 2, 3}
		strings = []string{
			"Pijamalı hasta, yağız şoföre çabucak güvendi.",
			"イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム",
			"The quick brown fox jumps over the lazy dog",
		}
		aData           iserialization.Data = []byte{49, 49, 49, 51, 49, 51, 49, 50, 51, 49, 51, 49, 51, 49, 51, 49, 51, 49}
		anInnerPortable *AnInnerPortable    = &AnInnerPortable{i: anInt, f: aFloat}
		portables                           = []serialization.Portable{anInnerPortable, anInnerPortable, anInnerPortable}

		aCustomStreamSerializable    CustomStreamSerializable    = CustomStreamSerializable{I: anInt, F: aFloat}
		aCustomByteArraySerializable CustomByteArraySerializable = CustomByteArraySerializable{I: anInt, F: aFloat}

		aLocalDate       = (types.LocalDate)(time.Date(2021, 6, 28, 0, 0, 0, 0, time.Local))
		aLocalTime       = (types.LocalTime)(time.Date(0, 1, 1, 11, 22, 41, 123456789, time.Local))
		aLocalDateTime   = (types.LocalDateTime)(time.Date(2021, 6, 28, 11, 22, 41, 123456789, time.Local))
		anOffsetDateTime = (types.OffsetDateTime)(time.Date(2021, 6, 28, 11, 22, 41, 123456789, time.FixedZone("", 18*60*60)))

		localDates = []types.LocalDate{
			(types.LocalDate)(time.Date(2021, 6, 28, 0, 0, 0, 0, time.Local)),
			(types.LocalDate)(time.Date(1923, 4, 23, 0, 0, 0, 0, time.Local)),
			(types.LocalDate)(time.Date(1938, 11, 10, 0, 0, 0, 0, time.Local)),
		}
		localTimes = []types.LocalTime{
			(types.LocalTime)(time.Date(0, 1, 1, 9, 5, 10, 123456789, time.Local)),
			(types.LocalTime)(time.Date(0, 1, 1, 18, 30, 55, 567891234, time.Local)),
			(types.LocalTime)(time.Date(0, 1, 1, 15, 44, 39, 192837465, time.Local)),
		}
		localDataTimes = []types.LocalDateTime{
			(types.LocalDateTime)(time.Date(1938, 11, 10, 9, 5, 10, 123456789, time.Local)),
			(types.LocalDateTime)(time.Date(1923, 4, 23, 15, 44, 39, 192837465, time.Local)),
			(types.LocalDateTime)(time.Date(2021, 6, 28, 18, 30, 55, 567891234, time.Local)),
		}
		offsetDataTimes = []types.OffsetDateTime{
			(types.OffsetDateTime)(time.Date(1938, 11, 10, 9, 5, 10, 123456789, time.FixedZone("", 18*60*60))),
			(types.OffsetDateTime)(time.Date(1923, 4, 23, 15, 44, 39, 192837465, time.FixedZone("", 5*60*60))),
			(types.OffsetDateTime)(time.Date(2021, 6, 28, 18, 30, 55, 567891234, time.FixedZone("", -10*60*60))),
		}

		aBigInteger = big.NewInt(1314432323232411)
		aClass      = "java.math.BigDecimal"
		aBigDecimal = types.NewDecimal(big.NewInt(31231), 0)
		decimals    = []types.Decimal{aBigDecimal, aBigDecimal, aBigDecimal}

		anIdentifiedDataSerializable = AnIdentifiedDataSerializable{bool: aBoolean, b: aByte,
			c: aChar, d: aDouble, s: aShort, f: aFloat, i: anInt, l: aLong, str: anSqlString, booleans: booleans,
			bytes: bytes, chars: chars, doubles: doubles, shorts: shorts, floats: floats, ints: ints, longs: longs, strings: strings,
			byteSize: byte(len(bytes)), bytesFully: bytes, bytesOffset: []byte{bytes[1], bytes[2]}, strBytes: nil, strChars: nil,
			unsignedByte: 227, unsignedShort: 32867, portableObject: anInnerPortable, identifiedDataSerializableObject: nil, customStreamSerializableObject: aCustomStreamSerializable,
			customByteArraySerializableObject: aCustomByteArraySerializable, data: aData}

		aPortable = &APortable{boolean: aBoolean, b: aByte, c: aChar, d: aDouble, s: aShort, f: aFloat, i: anInt, l: aLong,
			str: anSqlString, bd: aBigDecimal, ld: aLocalDate, lt: aLocalTime, ldt: aLocalDateTime, odt: anOffsetDateTime, p: anInnerPortable,
			booleans: booleans, bytes: bytes, chars: chars, doubles: doubles, shorts: shorts, floats: floats, ints: ints, longs: longs, strings: strings,
			decimals: decimals, dates: localDates, times: localTimes, dateTimes: localDataTimes, offsetDateTimes: offsetDataTimes, portables: portables,
			byteSize: byte(len(bytes)), bytesFully: bytes, bytesOffset: []byte{bytes[1], bytes[2]}, strBytes: nil, strChars: nil,
			unsignedByte: 227, unsignedShort: 32867, portableObject: anInnerPortable, identifiedDataSerializableObject: &anIdentifiedDataSerializable, customStreamSerializableObject: aCustomStreamSerializable,
			customByteArraySerializableObject: aCustomByteArraySerializable, data: aData,
		}
	)
	anIdentifiedDataSerializable.strChars = make([]uint16, len(anIdentifiedDataSerializable.str))
	for i, r := range anIdentifiedDataSerializable.str {
		anIdentifiedDataSerializable.strChars[i] = uint16(r)
	}
	anIdentifiedDataSerializable.strBytes = make([]byte, len(anIdentifiedDataSerializable.str))
	for i, r := range anIdentifiedDataSerializable.str {
		anIdentifiedDataSerializable.strBytes[i] = byte(r)
	}
	aPortable.strChars = make([]uint16, len(aPortable.str))
	for i, r := range aPortable.str {
		aPortable.strChars[i] = uint16(r)
	}
	aPortable.strBytes = make([]byte, len(aPortable.str))
	for i, r := range aPortable.str {
		aPortable.strBytes[i] = byte(r)
	}
	allTestObjects = map[string]interface{}{
		"NULL":            aNullObject,
		"Boolean":         aBoolean,
		"Byte":            aByte,
		"Character":       aChar,
		"Double":          aDouble,
		"Short":           aShort,
		"Float":           aFloat,
		"Integer":         anInt,
		"Long":            aLong,
		"String":          anSqlString,
		"UUID":            aUUID,
		"AnInnerPortable": anInnerPortable,

		"boolean[]": booleans,
		"byte[]":    bytes,
		"char[]":    chars,
		"double[]":  doubles,
		"short[]":   shorts,
		"float[]":   floats,
		"int[]":     ints,
		"long[]":    longs,
		"String[]":  strings,

		"CustomStreamSerializable":     aCustomStreamSerializable,
		"CustomByteArraySerializable":  aCustomByteArraySerializable,
		"APortable":                    aPortable,
		"AnIdentifiedDataSerializable": &anIdentifiedDataSerializable,

		"LocalDate":      aLocalDate,
		"LocalTime":      aLocalTime,
		"LocalDateTime":  aLocalDateTime,
		"OffsetDateTime": anOffsetDateTime,
		"BigInteger":     aBigInteger,
		"BigDecimal":     aBigDecimal,
		"Class":          aClass,

		"TruePredicate":        predicate.True(),
		"FalsePredicate":       predicate.False(),
		"SqlPredicate":         predicate.SQL(anSqlString),
		"EqualPredicate":       predicate.Equal(anSqlString, anInt),
		"NotEqualPredicate":    predicate.NotEqual(anSqlString, anInt),
		"GreaterLessPredicate": predicate.Greater(anSqlString, anInt),
		"BetweenPredicate":     predicate.Between(anSqlString, anInt, anInt),
		"LikePredicate":        predicate.Like(anSqlString, anSqlString),
		"ILikePredicate":       predicate.ILike(anSqlString, anSqlString),
		"InPredicate":          predicate.In(anSqlString, anInt, anInt),
		"RegexPredicate":       predicate.Regex(anSqlString, anSqlString),
		"AndPredicate": predicate.And(
			predicate.SQL(anSqlString),
			predicate.Equal(anSqlString, anInt),
			predicate.NotEqual(anSqlString, anInt),
			predicate.Greater(anSqlString, anInt),
			predicate.GreaterOrEqual(anSqlString, anInt),
		),
		"OrPredicate": predicate.Or(
			predicate.SQL(anSqlString),
			predicate.Equal(anSqlString, anInt),
			predicate.NotEqual(anSqlString, anInt),
			predicate.Greater(anSqlString, anInt),
			predicate.GreaterOrEqual(anSqlString, anInt),
		),
		"InstanceOfPredicate":      predicate.InstanceOf("com.hazelcast.nio.serialization.compatibility.CustomStreamSerializable"),
		"CountAggregator":          aggregate.Count(anSqlString),
		"DistinctValuesAggregator": aggregate.DistinctValues(anSqlString),
		"MaxAggregator":            aggregate.Max(anSqlString),
		"MinAggregator":            aggregate.Min(anSqlString),
		"DoubleSumAggregator":      aggregate.DoubleSum(anSqlString),
		"IntegerSumAggregator":     aggregate.IntSum(anSqlString),
		"LongSumAggregator":        aggregate.LongSum(anSqlString),
		"DoubleAverageAggregator":  aggregate.DoubleAverage(anSqlString),
		"IntegerAverageAggregator": aggregate.IntAverage(anSqlString),
		"LongAverageAggregator":    aggregate.LongAverage(anSqlString),
	}
	return allTestObjects
}
