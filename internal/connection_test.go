package internal

import (
	"testing"
)

func TestClientMessageReader(t *testing.T) {
	//given

	//when
	reader := clientMessageReader{}
	reader.append([]byte("test-1"))
	reader.append([]byte("test-2"))
	println(reader.chunksTotalSize)

	//then

}
