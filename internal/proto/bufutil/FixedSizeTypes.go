package bufutil

import (
	proto "github.com/gulcesirvanci/hazelcast-go-client/internal/proto/custom"
	_ "github.com/hazelcast/hazelcast-go-client/serialization"
)
/*
WIP
TODO: Need to fill functions.
 */

const (
	ByteSizeInBytes    = 1
	LongSizeInBytes    = Int64SizeInBytes
	IntSizeInBytes     = Int32SizeInBytes
	BooleanSizeInBytes = BoolSizeInBytes
	UUIDSizeInBytes    = Int64SizeInBytes * 2
)

/*   public static void encodeEnum(byte[] buffer, int pos, IndexType indexType) {
       encodeInt(buffer, pos, indexType.getId());
   }

   public static void encodeEnum(byte[] buffer, int pos, ExpiryPolicyType expiryPolicyType) {
       encodeInt(buffer, pos, expiryPolicyType.getId());
   }

   public static void encodeEnum(byte[] buffer, int pos, TimeUnit timeUnit) {
       int timeUnitId;
       if (TimeUnit.NANOSECONDS.equals(timeUnit)) {
           timeUnitId = 0;
       } else if (TimeUnit.MICROSECONDS.equals(timeUnit)) {
           timeUnitId = 1;
       } else if (TimeUnit.MILLISECONDS.equals(timeUnit)) {
           timeUnitId = 2;
       } else if (TimeUnit.SECONDS.equals(timeUnit)) {
           timeUnitId = 3;
       } else if (TimeUnit.MINUTES.equals(timeUnit)) {
           timeUnitId = 4;
       } else if (TimeUnit.HOURS.equals(timeUnit)) {
           timeUnitId = 5;
       } else if (TimeUnit.DAYS.equals(timeUnit)) {
           timeUnitId = 6;
       } else {
           timeUnitId = -1;
       }
       encodeInt(buffer, pos, timeUnitId);
   }

   public static void encodeEnum(byte[] buffer, int pos, ClientBwListEntryDTO.Type clientBwListEntryType) {
       encodeInt(buffer, pos, clientBwListEntryType.getId());
   }

   public static int decodeEnum(byte[] buffer, int pos) {
       return decodeInt(buffer, pos);
   }
 */

func EncodeInt(buffer []byte, pos int, value int) {
pos = value * 2
	//.WriteInt32(buffer, pos, value)
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

func EncodeUUID(buffer []byte, pos int, value string) { //UUID int64
	/*mostSigBits := value.getMostSignificantBits()
	leastSigBits := value.getLeastSignificantBits()
	encodeLong(buffer, pos, mostSigBits)
	encodeLong(buffer, pos+LONG_SIZE_IN_BYTES, leastSigBits)

	 */
}

func DecodeUUID(buffer []byte, pos int) string {
	/*mostSigBits := decodeLong(buffer, pos)
	leastSigBits := decodeLong(buffer, pos + LONG_SIZE_IN_BYTES)
	return newUUID(mostSigBits, leastSigBits)

	 */
	return "aaa"
}
