package hazelcast

// Must panics if err is not nil
func Must(err error) {
	if err != nil {
		panic(err)
	}
}

// MustValue returns value if err is nil, otherwise it panics.
func MustValue(value interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return value
}

// MustValue returns value if err is nil, otherwise it panics.
func MustBool(value bool, err error) bool {
	if err != nil {
		panic(err)
	}
	return value
}
