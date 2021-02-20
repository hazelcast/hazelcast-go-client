package internal

import (
	"bytes"
	"testing"
)

func TestClientMessageReader(t *testing.T) {
	//given

	//when
	reader := clientMessageReader{}
	reader.Append(bytes.NewBuffer([]byte("test-1")))
	reader.Append(bytes.NewBuffer([]byte("test-2")))
	println(reader.chunksTotalSize)

	//then

}
