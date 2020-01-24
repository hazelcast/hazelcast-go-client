package bufutil

type Uuid struct {
	msb int64
	lsb int64
}

func (u Uuid) GetLeastSignificantBits() int64 {
	return u.lsb
}

func (u Uuid) GetMostSignificantBits() int64 {
	return u.msb
}


type Pair struct {
	key, value interface{}
}

func NewPair(key interface{}, value interface{}) *Pair {
	return &Pair{key, value}
}

func (p *Pair) Key() interface{} {
	return p.key
}

func (p *Pair) Value() interface{} {
	return p.value
}
