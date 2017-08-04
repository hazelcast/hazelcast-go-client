package protocol

import (
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/serialization"
	."github.com/hazelcast/go-client/core"
)


func DataCalculateSize(d *Data) int {
	return len(d.Buffer) + INT_SIZE_IN_BYTES
}
func StringCalculateSize(str *string) int {
	return len(*str) + INT_SIZE_IN_BYTES
}
func AddressCalculateSize(a *Address) int {
	dataSize := 0
	dataSize += StringCalculateSize(&a.Host)
	dataSize += INT_SIZE_IN_BYTES
	return dataSize
}
