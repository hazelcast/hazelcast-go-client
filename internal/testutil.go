package internal

import (
	"math/rand"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func RandomString(length int) string {
	return StringWithCharset(length, charset)
}

func RandomClientAuthenticationRequestMessage(stringLen int) *proto.ClientMessage {
	var labels []string
	for i := 0; i < 10; i++ {
		labels = append(labels, RandomString(stringLen))
	}
	request := codec.EncodeClientAuthenticationRequest(
		RandomString(stringLen),
		RandomString(stringLen),
		RandomString(stringLen),
		types.NewUUID(),
		RandomString(stringLen),
		byte(1),
		RandomString(stringLen),
		RandomString(stringLen),
		labels,
	)
	return request
}
