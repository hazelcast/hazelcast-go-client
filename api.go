package hazelcast

//
//
type Address interface {
	Host() string
	Port() int32
}
type Member interface {
	Address() Address
	Uuid() string
	IsLiteMember() bool
	Attributes() map[string]string
}
type Pair interface {
	Key() interface{}
	Value() interface{}
}
type Data interface {
	Buffer() []byte
}
type DistributedObjectInfo interface {
	Name() string
	ServiceName() string
}
type Error interface {
	ErrorCode() int32
	ClassName() string
	Message() string
	StackTrace() []StackTraceElement
	CauseErrorCode() int32
	CauseClassName() string
}

type StackTraceElement interface {
	DeclaringClass() string
	MethodName() string
	FileName() string
	LineNumber() int32
}

type EntryView interface {
	Key() Data
	Value() Data
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
