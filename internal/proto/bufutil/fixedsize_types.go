package bufutil


const (
	ByteSizeInBytes    = 1
	LongSizeInBytes    = Int64SizeInBytes
	IntSizeInBytes     = Int32SizeInBytes
	BooleanSizeInBytes = BoolSizeInBytes
	UUIDSizeInBytes    = Int64SizeInBytes * 2
)

func EncodeInt(buffer []byte, pos int, value int32) {
	WriteInt32(buffer, pos, value, false)
}
func DecodeInt(buffer []byte, pos int) int32 {
	return ReadInt32(buffer, pos, false)
}

//TODO: Integer vs int

func EncodeLong(buffer []byte, pos int32, value int64) {
	WriteInt64(buffer, pos, value, false)
}
func DecodeLong(buffer []byte, pos int32) int64 {
	return ReadInt64(buffer, pos, false)
}

func EncodeBoolean(buffer []byte, pos int32, value bool) {
	WriteBool(buffer, pos, value)
}
func DecodeBoolean(buffer []byte, pos int) bool {
	return buffer[pos] == 1
}

func EncodeByte(buffer []byte, pos int, value byte) {
	buffer[pos] = value
}
func DecodeByte(buffer []byte, pos int) byte {
	return buffer[pos]
}

//TODO: how to
func EncodeUUID(buffer []byte, pos int32, value Uuid) { //UUID int64
	mostSigBits := value.GetMostSignificantBits()
	leastSigBits := value.GetLeastSignificantBits()
	EncodeLong(buffer, pos, mostSigBits)
	EncodeLong(buffer, pos+LongSizeInBytes, leastSigBits)

}

func DecodeUUID(buffer []byte, pos int32) Uuid {
	mostSigBits := DecodeLong(buffer, pos)
	leastSigBits := DecodeLong(buffer, pos+LongSizeInBytes)
	return Uuid{msb: mostSigBits, lsb:leastSigBits}

}
