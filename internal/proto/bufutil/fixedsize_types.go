package bufutil

import (
	_ "github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	ByteSizeInBytes    = 1
	LongSizeInBytes    = Int64SizeInBytes
	IntSizeInBytes     = Int32SizeInBytes
	BooleanSizeInBytes = BoolSizeInBytes
	UUIDSizeInBytes    = Int64SizeInBytes * 2
)

func EncodeInt(buffer []byte, pos int32, value int32) {
	WritInt32(buffer,pos,value,false)
}
func DecodeInt(buffer []byte, pos int32) int32 {
	return ReadInt32(buffer, pos,false)
}
//TODO: Integer vs int

func EncodeLong(buffer []byte, pos int32, value int64) {
	WriteInt64(buffer,pos,value,false)
}
func DecodeLong(buffer []byte, pos int32) int64 {
	return ReadInt64(buffer, pos,false)
}

func EncodeBoolean(buffer []byte, pos int32, value bool) {
	WriteBool(buffer,pos,value)
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

/*
func EncodeUUID(buffer []byte, pos int, value string) { //UUID int64
	mostSigBits := value.getMostSignificantBits()
	leastSigBits := value.getLeastSignificantBits()
	encodeLong(buffer, pos, mostSigBits)
	encodeLong(buffer, pos+LONG_SIZE_IN_BYTES, leastSigBits)


}

func DecodeUUID(buffer []byte, pos int) string {
	mostSigBits := decodeLong(buffer, pos)
	leastSigBits := decodeLong(buffer, pos + LONG_SIZE_IN_BYTES)
	return newUUID(mostSigBits, leastSigBits)


	return "aaa"
}

 */