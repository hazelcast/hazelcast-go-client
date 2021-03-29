package hztypes

// SimpleEntryView represents a readonly view of a map entry.
type SimpleEntryView struct {
	// TODO: export fields
	key            interface{}
	value          interface{}
	cost           int64
	creationTime   int64
	expirationTime int64
	hits           int64
	lastAccessTime int64
	lastStoredTime int64
	lastUpdateTime int64
	version        int64
	ttl            int64
	maxIdle        int64
}

func NewSimpleEntryView(key, value interface{}, cost, creationTime, expirationTime, hits, lastAccessTime,
	lastStoredTime, lastUpdateTime, version, ttl, maxIdle int64) *SimpleEntryView {
	return &SimpleEntryView{key, value, cost, creationTime, expirationTime,
		hits, lastAccessTime, lastStoredTime, lastUpdateTime,
		version, ttl, maxIdle}
}

func (s SimpleEntryView) Key() interface{} {
	return s.key
}

func (s SimpleEntryView) Value() interface{} {
	return s.value
}

func (s SimpleEntryView) Cost() int64 {
	return s.cost
}

func (s SimpleEntryView) CreationTime() int64 {
	return s.creationTime
}

func (s SimpleEntryView) ExpirationTime() int64 {
	return s.expirationTime
}

func (s SimpleEntryView) Hits() int64 {
	return s.hits
}

func (s SimpleEntryView) LastAccessTime() int64 {
	return s.lastAccessTime
}

func (s SimpleEntryView) LastStoredTime() int64 {
	return s.lastStoredTime
}

func (s SimpleEntryView) LastUpdateTime() int64 {
	return s.lastUpdateTime
}

func (s SimpleEntryView) Version() int64 {
	return s.version
}

func (s SimpleEntryView) Ttl() int64 {
	return s.ttl
}

func (s SimpleEntryView) MaxIdle() int64 {
	return s.maxIdle
}
