package bufutil

import (
	"encoding/binary"
	_ "github.com/hazelcast/hazelcast-go-client/serialization"
	"math"
)
/*
WIP
TODO: Need to fill functions.
*/

/*
const (
	ByteSizeInBytes    = 1
	LongSizeInBytes    = Int64SizeInBytes
	IntSizeInBytes     = Int32SizeInBytes
	BooleanSizeInBytes = BoolSizeInBytes
	UUIDSizeInBytes    = Int64SizeInBytes * 2
)*/

func WritInt32(buffer []byte, pos int32, value int32, isBigEndian bool){
	s := buffer[pos:]
	if isBigEndian {
		binary.BigEndian.PutUint32(s, uint32(value))
	} else {
		binary.LittleEndian.PutUint32(s, uint32(value))
	}
}

func ReadInt32(buffer []byte, pos int32, isBigEndian bool) int32 {
	if isBigEndian {
		return int32(binary.BigEndian.Uint32(buffer[pos:]))
	} else {
		return int32(binary.LittleEndian.Uint32(buffer[pos:]))
	}
}
func WriteInt64(buffer []byte, pos int32, value int64, isBigEndian bool){
	s := buffer[pos:]
	if isBigEndian {
		binary.BigEndian.PutUint64(s, uint64(value))
	} else {
		binary.LittleEndian.PutUint64(s, uint64(value))
	}
}

func ReadInt64(buffer []byte, pos int32, isBigEndian bool) int64 {
	if isBigEndian {
		return int64(binary.BigEndian.Uint64(buffer[pos:]))
	} else {
		return int64(binary.LittleEndian.Uint64(buffer[pos:]))
	}
}

func WriteFloat32(buf []byte, pos int32, v float32, isBigEndian bool) {
	s := buf[pos:]
	if isBigEndian {
		binary.BigEndian.PutUint32(s, math.Float32bits(v))
	} else {
		binary.LittleEndian.PutUint32(s, math.Float32bits(v))
	}
}

func ReadFloat32(buf []byte, pos int32, isBigEndian bool) float32 {
	if isBigEndian {
		return math.Float32frombits(binary.BigEndian.Uint32(buf[pos:]))
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(buf[pos:]))
}

func WriteFloat64(buf []byte, pos int32, v float64, isBigEndian bool) {
	s := buf[pos:]
	if isBigEndian {
		binary.BigEndian.PutUint64(s, math.Float64bits(v))
	} else {
		binary.LittleEndian.PutUint64(s, math.Float64bits(v))
	}
}

func ReadFloat64(buf []byte, pos int32, isBigEndian bool) float64 {
	if isBigEndian {
		return math.Float64frombits(binary.BigEndian.Uint64(buf[pos:]))
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(buf[pos:]))
}

func WriteBool(buf []byte, pos int32, v bool) {
	var b byte = 1
	if v {
		buf[pos] = b
	} else {
		b = 0
		buf[pos] = b
	}
}

func ReadBool(buf []byte, pos int32) bool {
	return buf[pos] == 1
}

func WriteUInt8(buf []byte, pos int32, v uint8) {
	buf[pos] = v
}

func ReadUInt8(buf []byte, pos int32) uint8 {
	return buf[pos]
}

func WriteUInt16(buf []byte, pos int32, v uint16, isBigEndian bool) {
	s := buf[pos:]
	if isBigEndian {
		binary.BigEndian.PutUint16(s, v)
	} else {
		binary.LittleEndian.PutUint16(s, v)
	}
}

func ReadUInt16(buf []byte, pos int32, isBigEndian bool) uint16 {
	if isBigEndian {
		return binary.BigEndian.Uint16(buf[pos:])
	}
	return binary.LittleEndian.Uint16(buf[pos:])
}

func WriteInt16(buf []byte, pos int32, v int16, isBigEndian bool) {
	s := buf[pos:]
	if isBigEndian {
		binary.BigEndian.PutUint16(s, uint16(v))
	} else {
		binary.LittleEndian.PutUint16(s, uint16(v))
	}
}

func ReadInt16(buf []byte, pos int32, isBigEndian bool) int16 {
	if isBigEndian {
		return int16(binary.BigEndian.Uint16(buf[pos:]))
	}
	return int16(binary.LittleEndian.Uint16(buf[pos:]))
}
