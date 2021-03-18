package proxy

const (
	EventEntryNotified = "internal.proxy.entrynotified"
)

type EntryNotifiedEventImpl struct {
	eventType    int32
	ownerName    string
	memberName   string
	key          interface{}
	value        interface{}
	oldValue     interface{}
	mergingValue interface{}
}

func (e *EntryNotifiedEventImpl) Name() string {
	return EventEntryNotified
}

func (e *EntryNotifiedEventImpl) EntryEventType() int32 {
	return e.eventType
}

func (e *EntryNotifiedEventImpl) OwnerName() string {
	return e.ownerName
}

func (e *EntryNotifiedEventImpl) MemberName() string {
	return e.memberName
}

func (e *EntryNotifiedEventImpl) Key() interface{} {
	return e.key
}

func (e *EntryNotifiedEventImpl) Value() interface{} {
	return e.value
}

func (e *EntryNotifiedEventImpl) OldValue() interface{} {
	return e.oldValue
}

func (e *EntryNotifiedEventImpl) MergingValue() interface{} {
	return e.mergingValue
}

func NewEntryNotifiedEventImpl(ownerName string, memberName string, key interface{}, value interface{}, oldValue interface{}, mergingValue interface{}) *EntryNotifiedEventImpl {
	return &EntryNotifiedEventImpl{
		ownerName:    ownerName,
		memberName:   memberName,
		key:          key,
		value:        value,
		oldValue:     oldValue,
		mergingValue: mergingValue,
	}
}
