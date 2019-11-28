package bufutil

import (
	_ "github.com/hazelcast/hazelcast-go-client/serialization"
)
/*
WIP
TODO: Need to fill functions.
 */

const (
	BytesSizeInBytes   = ByteSizeInBytes
	LongSizeInBytes    = Int64SizeInBytes
	IntSizeInBytes     = Int32SizeInBytes
	BooleanSizeInBytes = BoolSizeInBytes
	UUIDSizeInBytes    = Int64SizeInBytes * 2
)


func EncodeInt(buffer []byte, pos int, value int) {

	//cMessage.WriteInt32(buffer, pos, value)
}

func DecodeInt(buffer []byte, pos int) int {
	return 1//readInt32(buffer, pos)
}

func EncodeInteger(buffer []byte, pos int, value int64) {
	//writeInt64(buffer, pos, value)
}

func DecodeInteger(buffer []byte, pos int) int64 {
	return 1 //readInt64(buffer, pos)
}

func EncodeLong(buffer []byte, pos int, value int64) {
	//writeInt64(buffer, pos, value)
}

func DecodeLong(buffer []byte, pos int) int64 {
	return 1//readInt64(buffer, pos)
}

func EncodeBoolean(buffer []byte, pos int, value bool) {
	//buffer[pos] = (value ? 1 : 0) //(byte)
}

func DecodeBoolean(buffer []byte, pos int) bool {
	return buffer[pos] == 1 //(byte)
}

func EncodeByte(buffer []byte, pos int, value byte) {
	buffer[pos] = value
}

func DecodeByte(buffer []byte, pos int) byte {
	return buffer[pos]
}

func EncodeUUID(buffer []byte, pos int, value int64) { //UUID
	/*mostSigBits := value.getMostSignificantBits()
	leastSigBits := value.getLeastSignificantBits()
	encodeLong(buffer, pos, mostSigBits)
	encodeLong(buffer, pos+LONG_SIZE_IN_BYTES, leastSigBits)

	 */
}

func DecodeUUID(buffer []byte, pos int) int64 {
	/*mostSigBits := decodeLong(buffer, pos)
	leastSigBits := decodeLong(buffer, pos + LONG_SIZE_IN_BYTES)
	return newUUID(mostSigBits, leastSigBits)

	 */
	return 1
}
