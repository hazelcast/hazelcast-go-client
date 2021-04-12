package hztypes

// MustMap returns map if err is nil, otherwise it panics.
func MustMap(m Map, err error) Map {
	if err != nil {
		panic(err)
	}
	return m
}
