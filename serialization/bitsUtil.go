package serialization

import (
	"encoding/binary"
)

const (
	BYTE_SIZE_IN_BYTES    = 1
	BOOLEAN_SIZE_IN_BYTES = 1
	SHORT_SIZE_IN_BYTES   = 2
	CHAR_SIZE_IN_BYTES    = 2
	INT_SIZE_IN_BYTES     = 4
	FLOAT_SIZE_IN_BYTES   = 4
	INT64_SIZE_IN_BYTES   = 8
	DOUBLE_SIZE_IN_BYTES  = 8

	VERSION        = 0
	BEGIN_FLAG     = 0x80
	END_FLAG       = 0x40
	BEGIN_END_FLAG = BEGIN_FLAG | END_FLAG
	LISTENER_FLAG  = 0x01

	PAYLOAD_OFFSET = 18
	SIZE_OFFSET    = 0

	FRAME_LENGTH_FIELD_OFFSET   = 0
	VERSION_FIELD_OFFSET        = FRAME_LENGTH_FIELD_OFFSET + INT_SIZE_IN_BYTES
	FLAGS_FIELD_OFFSET          = VERSION_FIELD_OFFSET + BYTE_SIZE_IN_BYTES
	TYPE_FIELD_OFFSET           = FLAGS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES
	CORRELATION_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + SHORT_SIZE_IN_BYTES
	PARTITION_ID_FIELD_OFFSET   = CORRELATION_ID_FIELD_OFFSET + INT64_SIZE_IN_BYTES
	DATA_OFFSET_FIELD_OFFSET    = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES
	HEADER_SIZE                 = DATA_OFFSET_FIELD_OFFSET + SHORT_SIZE_IN_BYTES
)

func WriteInt(buf []byte, v int32, pos int, isBigIndian bool) {
	s := buf[pos:]
	if isBigIndian {
		binary.BigEndian.PutUint32(s, uint32(v))
	} else {
		binary.LittleEndian.PutUint32(s, uint32(v))
	}
}

func ReadInt(buf []byte, pos int, isBigIndian bool) int32 {
	if isBigIndian {
		return int32(binary.BigEndian.Uint32(buf[pos:]))
	} else {
		return int32(binary.LittleEndian.Uint32(buf[pos:]))
	}
}
