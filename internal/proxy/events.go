package proxy

import "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"

func newEntryNotifiedEventImpl(
	ownerName string,
	memberName string,
	key interface{},
	value interface{},
	oldValue interface{},
	mergingValue interface{},
	numberOfAffectedEntries int,
) *hztypes.EntryNotified {
	return &hztypes.EntryNotified{
		OwnerName:               ownerName,
		MemberName:              memberName,
		Key:                     key,
		Value:                   value,
		OldValue:                oldValue,
		MergingValue:            mergingValue,
		NumberOfAffectedEntries: numberOfAffectedEntries,
	}
}
