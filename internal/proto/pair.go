package proto

type Pair struct {
	key, value interface{}
}

func NewPair(key, value interface{}) Pair {
	return Pair{key, value}
}

func (p Pair) GetKey() interface{} {
	return p.key
}

func (p Pair) GetValue() interface{} {
	return p.value
}
