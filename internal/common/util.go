package common

import (
	"crypto/rand"
	"fmt"
	"io"
	"time"
)

func CheckNotNil(v interface{}) bool {
	return v != nil
}
func CheckNotEmpty(v []interface{}) bool {
	return len(v) > 0
}
func GetTimeInMilliSeconds(time int64, duration time.Duration) int64 {
	var timeInMillis int64 = time * duration.Nanoseconds() / (1000000)
	if time > 0 && timeInMillis == 0 {
		timeInMillis = 1
	}
	return timeInMillis
}

// NewUUID generates a random UUID according to RFC 4122
func NewUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	uuid[8] = uuid[8]&^0xc0 | 0x80
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}
