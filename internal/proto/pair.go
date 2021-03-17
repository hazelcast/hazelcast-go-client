package proto

type Pair struct {
	key, value interface{}
}

func NewPair(key, value interface{}) Pair {
	return Pair{key, value}
}

func (p Pair) Key() interface{} {
	return p.key
}

func (p Pair) Value() interface{} {
	return p.value
}
