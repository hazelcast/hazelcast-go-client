package internal

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io"
)

// UUID for generation UUID string
type UUID interface {
	String() string
	MostSignificantBits() uint64
	LeastSignificantBits() uint64
}

type UuidImpl struct {
	mostSigBits  uint64
	leastSigBits uint64
}

// NewUUID is used to generate a random UUID
func NewUUID() UUID {
	buf := make([]byte, 16)
	_, _ = io.ReadFull(rand.Reader, buf)
	buf[6] &= 0x0f // clear version
	buf[6] |= 0x40 // set to version 4
	buf[8] &= 0x3f // clear variant
	buf[8] |= 0x80 // set to IETF variant

	return UuidImpl{binary.BigEndian.Uint64(buf[0:8]),
		binary.BigEndian.Uint64(buf[8:])}
}

func NewUUIDWith(mostSigBits, leastSigBits uint64) UUID {
	return UuidImpl{mostSigBits, leastSigBits}
}

func NewUUIDFromString(uuidStr string) UUID {
	// TODO:
	return NewUUID()
}

func (u UuidImpl) String() string {
	return string(u.marshalText())
}

func (u UuidImpl) MostSignificantBits() uint64 {
	return u.mostSigBits
}

func (u UuidImpl) LeastSignificantBits() uint64 {
	return u.leastSigBits
}

func (u UuidImpl) marshalText() []byte {
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[0:8], u.mostSigBits)
	binary.BigEndian.PutUint64(data[8:16], u.leastSigBits)
	dst := make([]byte, 36)
	hex.Encode(dst, data[:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], data[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], data[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], data[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:], data[10:])
	return dst
}
