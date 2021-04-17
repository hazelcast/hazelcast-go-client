package types

type Entry struct {
	Key   interface{}
	Value interface{}
}

func NewEntry(key interface{}, value interface{}) Entry {
	return Entry{
		Key:   key,
		Value: value,
	}
}
