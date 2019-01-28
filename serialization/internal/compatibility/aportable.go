// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compatibility

import (
	"github.com/hazelcast/hazelcast-go-client/serialization"
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
	p    serialization.Portable

	booleans  []bool
	bytes     []byte
	chars     []uint16
	doubles   []float64
	shorts    []int16
	floats    []float32
	ints      []int32
	longs     []int64
	strings   []string
	portables []serialization.Portable

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

func (*aPortable) ClassID() int32 {
	return portableClassID
}

func (*aPortable) FactoryID() int32 {
	return portableFactoryID
}

func (p *aPortable) WritePortable(writer serialization.PortableWriter) error {
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
		writer.WriteNilPortable("p", portableFactoryID, portableClassID)
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

	return nil
}

func (p *aPortable) ReadPortable(reader serialization.PortableReader) error {
	p.bool = reader.ReadBool("bool")
	p.b = reader.ReadByte("b")
	p.c = reader.ReadUInt16("c")
	p.d = reader.ReadFloat64("d")
	p.s = reader.ReadInt16("s")
	p.f = reader.ReadFloat32("f")
	p.i = reader.ReadInt32("i")
	p.l = reader.ReadInt64("l")
	p.str = reader.ReadUTF("str")
	p.p = reader.ReadPortable("p")

	p.booleans = reader.ReadBoolArray("booleans")
	p.bytes = reader.ReadByteArray("bs")
	p.chars = reader.ReadUInt16Array("cs")
	p.doubles = reader.ReadFloat64Array("ds")
	p.shorts = reader.ReadInt16Array("ss")
	p.floats = reader.ReadFloat32Array("fs")
	p.ints = reader.ReadInt32Array("is")
	p.longs = reader.ReadInt64Array("ls")
	p.strings = reader.ReadUTFArray("strs")
	p.portables = reader.ReadPortableArray("ps")

	p.booleansNull = reader.ReadBoolArray("booleansNull")
	p.bytesNull = reader.ReadByteArray("bsNull")
	p.charsNull = reader.ReadUInt16Array("csNull")
	p.doublesNull = reader.ReadFloat64Array("dsNull")
	p.shortsNull = reader.ReadInt16Array("ssNull")
	p.floatsNull = reader.ReadFloat32Array("fsNull")
	p.intsNull = reader.ReadInt32Array("isNull")
	p.longsNull = reader.ReadInt64Array("lsNull")
	p.stringsNull = reader.ReadUTFArray("strsNull")

	return nil
}
