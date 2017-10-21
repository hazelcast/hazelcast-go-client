package core

type IAddress interface {
	Host() string
	Port() int
}
type IMember interface {
	Address() IAddress
	Uuid() string
	IsLiteMember() bool
	Attributes() map[string]string
}
type IPair interface {
	Key() interface{}
	Value() interface{}
}
type IData interface {
	Buffer() []byte
}
type IDistributedObjectInfo interface {
	Name() string
	ServiceName() string
}
type IError interface {
	ErrorCode() int32
	ClassName() string
	Message() string
	StackTrace() []IStackTraceElement
	CauseErrorCode() int32
	CauseClassName() string
}

type IStackTraceElement interface {
	DeclaringClass() string
	MethodName() string
	FileName() string
	LineNumber() int32
}

type IEntryView interface {
	Key() IData
	Value() IData
	Cost() int64
	CreationTime() int64
	ExpirationTime() int64
	Hits() int64
	LastAccessTime() int64
	LastStoredTime() int64
	LastUpdateTime() int64
	Version() int64
	EvictionCriteriaNumber() int64
	Ttl() int64
}
type IEntryEvent interface {
	KeyData() IData
	ValueData() IData
	OldValueData() IData
	MergingValueData() IData
	EventType() int32
	Uuid() *string
}
type IMapEvent interface {
	EventType() int32
	Uuid() *string
	NumberOfAffectedEntries() int32
}
type IEntryAddedListener interface {
	EntryAdded(IEntryEvent)
}
type IEntryRemovedListener interface {
	EntryRemoved(IEntryEvent)
}
type IEntryUpdatedListener interface {
	EntryUpdated(IEntryEvent)
}
type IEntryEvictedListener interface {
	EntryEvicted(IEntryEvent)
}
type IEntryEvictAllListener interface {
	EntryEvictAll(IMapEvent)
}
type IEntryClearAllListener interface {
	EntryClearAll(IMapEvent)
}
type IEntryMergedListener interface {
	EntryMerged(IEntryEvent)
}
type IEntryExpiredListener interface {
	EntryExpired(IEntryEvent)
}
type IMemberAddedListener interface {
	MemberAdded(member IMember)
}
type IMemberRemovedListener interface {
	MemberRemoved(member IMember)
}
type IEntryBackupProcessor interface {
	ProcessBackup(entry IPair)
}
type IEntryProcessor interface {
	Process(entry IPair)
}
type ILifecycleListener interface {
	LifecycleStateChanged(string)
}
