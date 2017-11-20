// Package serialization serializes user objects to Data and back to Object.
// Data is the internal representation of binary data in Hazelcast.
package serialization

// IdentifiedDataSerializableFactory is used to create IdentifiedDataSerializable instances during deserialization.
type IdentifiedDataSerializableFactory interface {
	// Creates an IdentifiedDataSerializable instance using given type ID.
	Create(id int32) IdentifiedDataSerializable
}

// IdentifiedDataSerializable is a serialization method as an alternative to standard Gob serialization.
// Each IdentifiedDataSerializable is created by a registered IdentifiedDataSerializableFactory.
type IdentifiedDataSerializable interface {
	// ReadData reads fields from the input stream.
	ReadData(input DataInput) error

	// WriteData writes object fields to output stream.
	WriteData(output DataOutput) error

	// FactoryId returns IdentifiedDataSerializableFactory factory ID for this struct.
	FactoryId() int32

	// ClassId returns type identifier for this struct. It should be unique per IdentifiedDataSerializableFactory.
	ClassId() int32
}

// Portable provides an alternative serialization method. Instead of relying on reflection, each Portable is
// created by a registered PortableFactory.
// Portable serialization has the following advantages:
//
// * Supporting multiversion of the same object type.
//
// * Fetching individual fields without having to rely on reflection.
//
// * Querying and indexing support without deserialization and/or reflection.
type Portable interface {
	// FactoryId returns PortableFactory ID for this portable struct.
	FactoryId() int32

	// ClassId returns type identifier for this portable struct. Class ID should be unique per PortableFactory.
	ClassId() int32

	// WritePortable serializes this portable object using PortableWriter.
	WritePortable(writer PortableWriter)

	// ReadPortable reads portable fields using PortableReader.
	ReadPortable(reader PortableReader)
}

// VersionedPortable is an extension to Portable
// to support per class version instead of a global serialization version.
type VersionedPortable interface {
	Portable

	// Version returns version for this Portable struct.
	Version() int32
}

// PortableFactory is used to create Portable instances during deserialization.
type PortableFactory interface {
	// Create creates a Portable instance using given class ID and
	// returns portable instance or nil if class ID is not known by this factory.
	Create(classId int32) Portable
}

// Serializer is base interface of serializers.
type Serializer interface {
	// Id returns id of serializer.
	Id() int32

	// Read reads object from ObjectDataInput.
	Read(input DataInput) (interface{}, error)

	// Write writes object to ObjectDataOutput.
	Write(output DataOutput, object interface{}) error
}

// IData is the basic unit of serialization. It stores binary form of an object serialized
// by SerializationService's ToData() method.
type IData interface {
	// Returns byte array representation of internal binary format.
	Buffer() []byte

	// Returns serialization type of binary form.
	GetType() int32

	// Returns the total size of Data in bytes.
	TotalSize() int

	// Returns size of internal binary data in bytes.
	DataSize() int

	// Returns partition hash calculated for serialized object.
	GetPartitionHash() int32
}

// DataOutput provides serialization methods.
type DataOutput interface {
	// Returns the head position in the byte array.
	Position() int32

	// Sets the head position in the byte array.
	SetPosition(pos int32)

	// Writes a byte.
	WriteByte(v byte)

	// Writes a bool.
	WriteBool(v bool)

	// Writes an uint16.
	WriteUInt16(v uint16)

	// Writes an int16.
	WriteInt16(v int16)

	// Writes an int32.
	WriteInt32(v int32)

	// Writes an int64.
	WriteInt64(v int64)

	// Writes a float32.
	WriteFloat32(v float32)

	// Writes a float64.
	WriteFloat64(v float64)

	// Writes a string in UTF-8 format.
	WriteUTF(v string)

	// Writes an object.
	WriteObject(i interface{}) error

	// Writes an IData.
	WriteData(data IData)

	// Writes a []byte.
	WriteByteArray(v []byte)

	// Writes a []bool.
	WriteBoolArray(v []bool)

	// Writes an []uint16.
	WriteUInt16Array(v []uint16)

	// Writes an []int16.
	WriteInt16Array(v []int16)

	// Writes an []int32.
	WriteInt32Array(v []int32)

	// Writes an []int64.
	WriteInt64Array(v []int64)

	// Writes a []float32.
	WriteFloat32Array(v []float32)

	// Writes a []float64.
	WriteFloat64Array(v []float64)

	// Writes a []string in UTF-8 format.
	WriteUTFArray(v []string)

	// Writes a string's characters.
	WriteBytes(bytes string)

	// Writes zero bytes as given length.
	WriteZeroBytes(count int)
}

// PositionalDataOutput provides some serialization methods for a specific position.
type PositionalDataOutput interface {
	// Provides serialization methods.
	DataOutput

	// Writes a byte to a specific position.
	PWriteByte(position int32, v byte)

	// Writes a bool to a specific position.
	PWriteBool(position int32, v bool)

	// Writes an uint16 to a specific position.
	PWriteUInt16(position int32, v uint16)

	// Writes an int16 to a specific position.
	PWriteInt16(position int32, v int16)

	// Writes an int32 to a specific position.
	PWriteInt32(position int32, v int32)

	// Writes an int64 to a specific position.
	PWriteInt64(position int32, v int64)

	// Writes a float32 to a specific position.
	PWriteFloat32(position int32, v float32)

	// Writes a float64 to a specific position.
	PWriteFloat64(position int32, v float64)
}

// DataInput provides deserialization methods.
type DataInput interface {
	// Returns the head position in the byte array.
	Position() int32

	// Sets the head position in the byte array.
	SetPosition(pos int32)

	// Returns byte read and error.
	ReadByte() (byte, error)

	// Returns bool read and error.
	ReadBool() (bool, error)

	// Returns uint16 read and error.
	ReadUInt16() (uint16, error)

	// Returns int16 read and error.
	ReadInt16() (int16, error)

	// Returns int32 read and error.
	ReadInt32() (int32, error)

	// Returns int64 read and error.
	ReadInt64() (int64, error)

	// Returns float32 read and error.
	ReadFloat32() (float32, error)

	// Returns float64 read and error.
	ReadFloat64() (float64, error)

	// Returns string read and error.
	ReadUTF() (string, error)

	// Returns object read and error.
	ReadObject() (interface{}, error)

	// Returns IData read and error.
	ReadData() (IData, error)

	// Returns []byte read and error.
	ReadByteArray() ([]byte, error)

	// Returns []bool read and error.
	ReadBoolArray() ([]bool, error)

	// Returns []uint16 read and error.
	ReadUInt16Array() ([]uint16, error)

	// Returns []int16 read and error.
	ReadInt16Array() ([]int16, error)

	// Returns []int32 read and error.
	ReadInt32Array() ([]int32, error)

	// Returns []int64 read and error.
	ReadInt64Array() ([]int64, error)

	// Returns []float32 read and error.
	ReadFloat32Array() ([]float32, error)

	// Returns []float64 read and error.
	ReadFloat64Array() ([]float64, error)

	// Returns []string read and error.
	ReadUTFArray() ([]string, error)
}

// Provides a mean of writing portable fields to a binary in form of go primitives
// arrays of go primitives, nested portable fields and array of portable fields.
type PortableWriter interface {
	// Writes a byte with fieldName.
	WriteByte(fieldName string, value byte)

	// Writes a bool with fieldName.
	WriteBool(fieldName string, value bool)

	// Writes a uint16 with fieldName.
	WriteUInt16(fieldName string, value uint16)

	// Writes a int16 with fieldName.
	WriteInt16(fieldName string, value int16)

	// Writes a int32 with fieldName.
	WriteInt32(fieldName string, value int32)

	// Writes a int64 with fieldName.
	WriteInt64(fieldName string, value int64)

	// Writes a float32 with fieldName.
	WriteFloat32(fieldName string, value float32)

	// Writes a float64 with fieldName.
	WriteFloat64(fieldName string, value float64)

	// Writes a string in UTF-8 format with fieldName.
	WriteUTF(fieldName string, value string)

	// Writes a Portable with fieldName.
	WritePortable(fieldName string, value Portable) error

	// Writes a NilPortable with fieldName, factoryId and classId.
	WriteNilPortable(fieldName string, factoryId int32, classId int32) error

	// Writes a []byte with fieldName.
	WriteByteArray(fieldName string, value []byte)

	// Writes a []bool with fieldName.
	WriteBoolArray(fieldName string, value []bool)

	// Writes a []uint16 with fieldName.
	WriteUInt16Array(fieldName string, value []uint16)

	// Writes a []int16 with fieldName.
	WriteInt16Array(fieldName string, value []int16)

	// Writes a []int32 with fieldName.
	WriteInt32Array(fieldName string, value []int32)

	// Writes a []int64 with fieldName.
	WriteInt64Array(fieldName string, value []int64)

	// Writes a []float32 with fieldName.
	WriteFloat32Array(fieldName string, value []float32)

	// Writes a []float64 with fieldName.
	WriteFloat64Array(fieldName string, value []float64)

	// Writes a []string in UTF-8 format with fieldName.
	WriteUTFArray(fieldName string, value []string)

	// Writes a []Portable with fieldName.
	WritePortableArray(fieldName string, value []Portable) error

	// Should not be called by the end user.
	End()
}

// Provides a mean of reading portable fields from a binary in form of go primitives
// arrays of go primitives, nested portable fields and array of portable fields.
type PortableReader interface {
	// It takes fieldName name of the field and returns the byte value read and error.
	ReadByte(fieldName string) (byte, error)

	// It takes fieldName name of the field and returns the bool value read and error.
	ReadBool(fieldName string) (bool, error)

	// It takes fieldName name of the field and returns the uint16 value read and error.
	ReadUInt16(fieldName string) (uint16, error)

	// It takes fieldName name of the field and returns the int16 value read and error.
	ReadInt16(fieldName string) (int16, error)

	// It takes fieldName name of the field and returns the int32 value read and error.
	ReadInt32(fieldName string) (int32, error)

	// It takes fieldName name of the field and returns the int64 value read and error.
	ReadInt64(fieldName string) (int64, error)

	// It takes fieldName name of the field and returns the float32 value read and error.
	ReadFloat32(fieldName string) (float32, error)

	// It takes fieldName name of the field and returns the float64 value read and error.
	ReadFloat64(fieldName string) (float64, error)

	// It takes fieldName name of the field and returns the string value read and error.
	ReadUTF(fieldName string) (string, error)

	// It takes fieldName name of the field and returns the Portable value read and error.
	ReadPortable(fieldName string) (Portable, error)

	// It takes fieldName name of the field and returns the []byte value read and error.
	ReadByteArray(fieldName string) ([]byte, error)

	// It takes fieldName name of the field and returns the []bool value read and error.
	ReadBoolArray(fieldName string) ([]bool, error)

	// It takes fieldName name of the field and returns the []uint16 value read and error.
	ReadUInt16Array(fieldName string) ([]uint16, error)

	// It takes fieldName name of the field and returns the []int16 value read and error.
	ReadInt16Array(fieldName string) ([]int16, error)

	// It takes fieldName name of the field and returns the []int32 value read and error.
	ReadInt32Array(fieldName string) ([]int32, error)

	// It takes fieldName name of the field and returns the []int64 value read and error.
	ReadInt64Array(fieldName string) ([]int64, error)

	// It takes fieldName name of the field and returns the []float32 value read and error.
	ReadFloat32Array(fieldName string) ([]float32, error)

	// It takes fieldName name of the field and returns the []float64 value read and error.
	ReadFloat64Array(fieldName string) ([]float64, error)

	// It takes fieldName name of the field and returns the []string value read and error.
	ReadUTFArray(fieldName string) ([]string, error)

	// It takes fieldName name of the field and returns the []Portable value read and error.
	ReadPortableArray(fieldName string) ([]Portable, error)

	// Should not be called by the end user.
	End()
}

// Represents a predicate (boolean-valued function) of one argument.
type IPredicate interface {
	// IPredicate implements IdentifiedDataSerializable interface.
	IdentifiedDataSerializable
}
