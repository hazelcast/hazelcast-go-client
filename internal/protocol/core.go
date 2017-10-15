package protocol

import (
	"bytes"
	"github.com/hazelcast/go-client/core"
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/serialization"
	. "github.com/hazelcast/go-client/serialization"
	"reflect"
)

type Address struct {
	host string
	port int
}

func NewAddress() *Address {
	return &Address{"localhost", 5701}
}
func NewAddressWithParameters(Host string, Port int) *Address {
	return &Address{Host, Port}
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

func NewMember(address Address, uuid string, isLiteMember bool, attributes map[string]string) *Member {
	return &Member{address: address, uuid: uuid, isLiteMember: isLiteMember, attributes: attributes}
}

func (member1 *Member) Address() core.IAddress {
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
	key, value interface{}
}

func NewPair(key interface{}, value interface{}) *Pair {
	return &Pair{key, value}
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
	if !reflect.DeepEqual(member1.attributes, member2.attributes) {
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

func (ev1 *EntryView) Key() IData {
	return ev1.key
}

func (ev1 *EntryView) Value() IData {
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
	stackTrace     []*StackTraceElement
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

func (err *Error) StackTrace() []core.IStackTraceElement {
	iStackTrace := make([]core.IStackTraceElement, len(err.stackTrace))
	for i, v := range err.stackTrace {
		iStackTrace[i] = core.IStackTraceElement(v)
	}
	return iStackTrace
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

type EntryEvent struct {
	keyData          *Data
	valueData        *Data
	oldValueData     *Data
	mergingValueData *Data
	eventType        int32
	uuid             *string
}

func (entryEvent *EntryEvent) KeyData() IData {
	return entryEvent.keyData
}

func (entryEvent *EntryEvent) ValueData() IData {
	return entryEvent.valueData
}

func (entryEvent *EntryEvent) OldValueData() IData {
	return entryEvent.oldValueData
}

func (entryEvent *EntryEvent) MergingValueData() IData {
	return entryEvent.mergingValueData
}

func (entryEvent *EntryEvent) Uuid() *string {
	return entryEvent.uuid
}
func (entryEvent *EntryEvent) EventType() int32 {
	return entryEvent.eventType
}

func NewEntryEvent(keyData *Data, valueData *Data, oldValueData *Data, mergingValueData *Data, eventType int32, Uuid *string) *EntryEvent {
	return &EntryEvent{keyData: keyData, valueData: valueData, oldValueData: oldValueData, eventType: eventType, uuid: Uuid}
}

type MapEvent struct {
	eventType               int32
	uuid                    *string
	numberOfAffectedEntries int32
}

func (mapEvent *MapEvent) Uuid() *string {
	return mapEvent.uuid
}

func (mapEvent *MapEvent) NumberOfAffectedEntries() int32 {
	return mapEvent.numberOfAffectedEntries
}
func (mapEvent *MapEvent) EventType() int32 {
	return mapEvent.eventType
}
func NewMapEvent(eventType int32, Uuid *string, numberOfAffectedEntries int32) *MapEvent {
	return &MapEvent{eventType: eventType, uuid: Uuid, numberOfAffectedEntries: numberOfAffectedEntries}
}

type EntryAddedListener interface {
	EntryAdded(core.IEntryEvent)
}
type EntryRemovedListener interface {
	EntryRemoved(core.IEntryEvent)
}
type EntryUpdatedListener interface {
	EntryUpdated(core.IEntryEvent)
}
type EntryEvictedListener interface {
	EntryEvicted(core.IEntryEvent)
}
type EntryEvictAllListener interface {
	EntryEvictAll(core.IMapEvent)
}
type EntryClearAllListener interface {
	EntryClearAll(core.IMapEvent)
}
type EntryMergedListener interface {
	EntryMerged(core.IEntryEvent)
}
type EntryExpiredListener interface {
	EntryExpired(core.IEntryEvent)
}
type DecodeListenerResponse func(message *ClientMessage) *string
type EncodeListenerRemoveRequest func(registrationId *string) *ClientMessage
type MemberAddedListener interface {
	MemberAdded(member core.IMember)
}
type MemberRemovedListener interface {
	MemberRemoved(member core.IMember)
}

// Helper function to get flags for listeners
func GetEntryListenerFlags(listener interface{}) int32 {
	flags := int32(0)
	if _, ok := listener.(EntryAddedListener); ok {
		flags |= ENTRYEVENT_ADDED
	}
	if _, ok := listener.(EntryRemovedListener); ok {
		flags |= ENTRYEVENT_REMOVED
	}
	if _, ok := listener.(EntryUpdatedListener); ok {
		flags |= ENTRYEVENT_UPDATED
	}
	if _, ok := listener.(EntryEvictedListener); ok {
		flags |= ENTRYEVENT_EVICTED
	}
	if _, ok := listener.(EntryEvictAllListener); ok {
		flags |= ENTRYEVENT_EVICT_ALL
	}
	if _, ok := listener.(EntryClearAllListener); ok {
		flags |= ENTRYEVENT_CLEAR_ALL
	}
	if _, ok := listener.(EntryExpiredListener); ok {
		flags |= ENTRYEVENT_EXPIRED
	}
	if _, ok := listener.(EntryMergedListener); ok {
		flags |= ENTRYEVENT_MERGED
	}
	return flags
}
