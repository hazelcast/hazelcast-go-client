package hztypes

type Pair struct {
	Key   interface{}
	Value interface{}
}

func NewPair(key interface{}, value interface{}) Pair {
	return Pair{
		Key:   key,
		Value: value,
	}
}
