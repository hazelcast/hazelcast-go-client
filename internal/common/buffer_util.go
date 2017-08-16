package common

import (
	"encoding/binary"
	"math"
)

func WriteInt32(buf []byte, pos int, v int32, isBigIndian bool) {
	s := buf[pos:]
	if isBigIndian {
		binary.BigEndian.PutUint32(s, uint32(v))
	} else {
		binary.LittleEndian.PutUint32(s, uint32(v))
	}
}

func ReadInt32(buf []byte, pos int, isBigIndian bool) int32 {
	if isBigIndian {
		return int32(binary.BigEndian.Uint32(buf[pos:]))
	} else {
		return int32(binary.LittleEndian.Uint32(buf[pos:]))
	}
}

func WriteFloat32(buf []byte, pos int, v float32, isBigIndian bool) {
	s := buf[pos:]
	if isBigIndian {
		binary.BigEndian.PutUint32(s, math.Float32bits(v))
	} else {
		binary.LittleEndian.PutUint32(s, math.Float32bits(v))
	}
}

func ReadFloat32(buf []byte, pos int, isBigIndian bool) float32 {
	if isBigIndian {
		return math.Float32frombits(binary.BigEndian.Uint32(buf[pos:]))
	} else {
		return math.Float32frombits(binary.LittleEndian.Uint32(buf[pos:]))
	}
}

func WriteFloat64(buf []byte, pos int, v float64, isBigIndian bool) {
	s := buf[pos:]
	if isBigIndian {
		binary.BigEndian.PutUint64(s, math.Float64bits(v))
	} else {
		binary.LittleEndian.PutUint64(s, math.Float64bits(v))
	}
}

func ReadFloat64(buf []byte, pos int, isBigIndian bool) float64 {
	if isBigIndian {
		return math.Float64frombits(binary.BigEndian.Uint64(buf[pos:]))
	} else {
		return math.Float64frombits(binary.LittleEndian.Uint64(buf[pos:]))
	}
}

func WriteBool(buf []byte, pos int, v bool) {
	var b byte = 1
	if v {
		buf[pos] = b
	} else {
		b = 0
		buf[pos] = b
	}
}

func ReadBool(buf []byte, pos int) bool {
	if buf[pos] == 1 {
		return true
	} else {
		return false
	}
}

func WriteUInt8(buf []byte, pos int, v uint8) {
	buf[pos] = v
}

func ReadUInt8(buf []byte, pos int) byte {
	return buf[pos]
}

func WriteInt16(buf []byte, pos int, v int16, isBigIndian bool) {
	s := buf[pos:]
	if isBigIndian {
		binary.BigEndian.PutUint16(s, uint16(v))
	} else {
		binary.LittleEndian.PutUint16(s, uint16(v))
	}
}

func ReadInt16(buf []byte, pos int, isBigIndian bool) int16 {
	if isBigIndian {
		return int16(binary.BigEndian.Uint16(buf[pos:]))
	} else {
		return int16(binary.LittleEndian.Uint16(buf[pos:]))
	}
}

func WriteInt64(buf []byte, pos int, v int64, isBigIndian bool) {
	s := buf[pos:]
	if isBigIndian {
		binary.BigEndian.PutUint64(s, uint64(v))
	} else {
		binary.LittleEndian.PutUint64(s, uint64(v))
	}
}

func ReadInt64(buf []byte, pos int, isBigIndian bool) int64 {
	if isBigIndian {
		return int64(binary.BigEndian.Uint64(buf[pos:]))
	} else {
		return int64(binary.LittleEndian.Uint64(buf[pos:]))
	}
}
