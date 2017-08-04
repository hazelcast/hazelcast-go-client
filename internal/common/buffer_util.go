package common

import (
	"encoding/binary"
	"math"
)

func WriteInt32(buf []byte, v int32, pos int, isBigIndian bool) {
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

func WriteFloat64(buf []byte, v float64, pos int, isBigIndian bool) {
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

