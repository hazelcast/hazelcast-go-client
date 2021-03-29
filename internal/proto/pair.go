package proto

type Pair struct {
	// TODO: export key and value
	key, value interface{}
}

// TODO: remove NewPair
func NewPair(key, value interface{}) Pair {
	return Pair{key, value}
}

// TODO: remove Key()
func (p Pair) Key() interface{} {
	return p.key
}

// TODO: remove Value()
func (p Pair) Value() interface{} {
	return p.value
}
