package serialization

type DataEntryView struct {
	keyData                Data
	valueData              Data
	cost                   int64
	creationTime           int64
	expirationTime         int64
	hits                   int64
	lastAccessTime         int64
	lastStoredTime         int64
	lastUpdateTime         int64
	version                int64
	evictionCriteriaNumber int64
	ttl                    int64
}

func (ev *DataEntryView) KeyData() Data {
	return ev.keyData
}

func (ev *DataEntryView) ValueData() Data {
	return ev.valueData
}

func (ev *DataEntryView) Cost() int64 {
	return ev.cost
}

func (ev *DataEntryView) CreationTime() int64 {
	return ev.creationTime
}

func (ev *DataEntryView) ExpirationTime() int64 {
	return ev.expirationTime
}

func (ev *DataEntryView) Hits() int64 {
	return ev.hits
}

func (ev *DataEntryView) LastAccessTime() int64 {
	return ev.lastAccessTime
}

func (ev *DataEntryView) LastStoredTime() int64 {
	return ev.lastStoredTime
}

func (ev *DataEntryView) LastUpdateTime() int64 {
	return ev.lastUpdateTime
}

func (ev *DataEntryView) Version() int64 {
	return ev.version
}

func (ev *DataEntryView) EvictionCriteriaNumber() int64 {
	return ev.evictionCriteriaNumber
}

func (ev *DataEntryView) TTL() int64 {
	return ev.ttl
}
