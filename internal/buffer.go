package internal

type Buffer struct {
	buf      []byte
	off		   int
	position   int
}

func (b Buffer) get(dest []byte, offset int, length int) {
	if (offset | length | (offset + length) | (len(dest) - (offset + length))) < 0 {
			panic("checkBounds: error")
	}
	remaining := len(dest) - b.position
	if length > remaining {
		panic("bufferUnderFlow : error")
	}
	end := offset + length
	for  i := offset; i < end; i++ {
		dest[i] = b.Get()
		return
	}
}

func (b Buffer)Get() byte {
	return b.buf[b.off+ len(b.buf) -1]
}

func (b Buffer) put(src []byte, offset int, length int) {
	if (offset | length | (offset + length) | (len(src) - (offset + length))) < 0 {
		panic("checkBounds: error")
	}
	remaining := len(src) - b.position
	if length > remaining {
		panic("bufferUnderFlow : error")
	}
	end := offset + length
	for  i := offset; i < end; i++ {
		b.Put(src[i])
		return
	}
}

func (b Buffer)Put(x byte) []byte {
 b.buf[b.off+ len(b.buf) -1] = x
	return b.buf
}


