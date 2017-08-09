package protocol

import (
	"bytes"
	"reflect"
	. "github.com/hazelcast/go-client/internal/serialization"
)

type Address struct {
	host string
	port int
}
func NewAddress() *Address{
	return &Address{"127.0.0.1",5701}
}
func NewAddressWithParameters(Host string,Port int) *Address{
	return &Address{Host,Port}
}
func (address *Address) Host() string {
	return address.host
}

func (address *Address) Port() int {
	return address.port
}

type Uuid struct {
	Msb int64
	Lsb int64
}
type Member struct {
	address      Address
	uuid         string
	isLiteMember bool
	attributes   map[string]string
}

func (member1 *Member) Address() *Address {
	return &member1.address
}

func (member1 *Member) Uuid() string {
	return member1.uuid
}

func (member1 *Member) IsLiteMember() bool {
	return member1.isLiteMember
}

func (member1 *Member) Attributes() map[string]string {
	return member1.attributes
}

type Pair struct {
	key,value interface{}
}

func (pair *Pair) Key() interface{} {
	return pair.key
}

func (pair *Pair) Value() interface{} {
	return pair.value
}

func (member1 *Member) Equal(member2 Member) bool {
	if member1.address != member2.address {
		return false
	}
	if member1.uuid != member2.uuid {
		return false
	}
	if member1.isLiteMember != member2.isLiteMember {
		return false
	}
	if !reflect.DeepEqual(member1.Attributes, member2.Attributes) {
		return false
	}
	return true
}

type DistributedObjectInfo struct {
	name        string
	serviceName string
}

func (obj *DistributedObjectInfo) Name() string {
	return obj.name
}

func (obj *DistributedObjectInfo) ServiceName() string {
	return obj.serviceName
}

type EntryView struct {
	key                    Data
	value                  Data
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

func (ev1 *EntryView) Key() Data {
	return ev1.key
}

func (ev1 *EntryView) Value() Data {
	return ev1.value
}

func (ev1 *EntryView) Cost() int64 {
	return ev1.cost
}

func (ev1 *EntryView) CreationTime() int64 {
	return ev1.creationTime
}

func (ev1 *EntryView) ExpirationTime() int64 {
	return ev1.expirationTime
}

func (ev1 *EntryView) Hits() int64 {
	return ev1.hits
}

func (ev1 *EntryView) LastAccessTime() int64 {
	return ev1.lastAccessTime
}

func (ev1 *EntryView) LastStoredTime() int64 {
	return ev1.lastStoredTime
}

func (ev1 *EntryView) LastUpdateTime() int64 {
	return ev1.lastUpdateTime
}

func (ev1 *EntryView) Version() int64 {
	return ev1.version
}

func (ev1 *EntryView) EvictionCriteriaNumber() int64 {
	return ev1.evictionCriteriaNumber
}

func (ev1 *EntryView) Ttl() int64 {
	return ev1.ttl
}

func (ev1 EntryView) Equal(ev2 EntryView) bool {
	if !bytes.Equal(ev1.key.Buffer(), ev2.key.Buffer()) || !bytes.Equal(ev1.value.Buffer(), ev2.value.Buffer()) {
		return false
	}
	if ev1.cost != ev2.cost || ev1.creationTime != ev2.creationTime || ev1.expirationTime != ev2.expirationTime || ev1.hits != ev2.hits {
		return false
	}
	if ev1.lastAccessTime != ev2.lastAccessTime || ev1.lastStoredTime != ev2.lastStoredTime || ev1.lastUpdateTime != ev2.lastUpdateTime {
		return false
	}
	if ev1.version != ev2.version || ev1.evictionCriteriaNumber != ev2.evictionCriteriaNumber || ev1.ttl != ev2.ttl {
		return false
	}
	return true
}

type Error struct {
	errorCode      int32
	className      string
	message        string
	stackTrace     []StackTraceElement
	causeErrorCode int32
	causeClassName string
}

func (err *Error) Error() string {
	return err.message
}

func (err *Error) ErrorCode() int32 {
	return err.errorCode
}

func (err *Error) ClassName() string {
	return err.className
}

func (err *Error) Message() string {
	return err.message
}

func (err *Error) StackTrace() []StackTraceElement {
	return err.stackTrace
}


func (err *Error) CauseErrorCode() int32 {
	return err.causeErrorCode
}

func (err *Error) CauseClassName() string {
	return err.causeClassName
}

type StackTraceElement struct {
	declaringClass string
	methodName     string
	fileName       string
	lineNumber     int32
}

func (st *StackTraceElement) DeclaringClass() string {
	return st.declaringClass
}

func (st *StackTraceElement) MethodName() string {
	return st.methodName
}

func (st *StackTraceElement) FileName() string {
	return st.fileName
}

func (st *StackTraceElement) LineNumber() int32 {
	return st.lineNumber
}


