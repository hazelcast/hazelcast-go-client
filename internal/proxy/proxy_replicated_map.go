package proxy

import (
	"github.com/hazelcast/hazelcast-go-client/hztypes"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

type ReplicatedMapImpl struct {
	*Proxy
	referenceIDGenerator ReferenceIDGenerator
	partitionID          int32
}

func NewReplicatedMapImpl(proxy *Proxy, partitionID int32) *ReplicatedMapImpl {
	return &ReplicatedMapImpl{
		Proxy:                proxy,
		referenceIDGenerator: NewReferenceIDGeneratorImpl(),
		partitionID:          partitionID,
	}
}

func (m ReplicatedMapImpl) Clear() error {
	request := codec.EncodeReplicatedMapClearRequest(m.Name)
	_, err := m.InvokeOnRandomTarget(request, nil)
	return err
}

func (m ReplicatedMapImpl) ContainsKey(key interface{}) (bool, error) {
	if keyData, err := m.ValidateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeReplicatedMapContainsKeyRequest(m.Name, keyData)
		if response, err := m.InvokeOnKey(request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeReplicatedMapContainsKeyResponse(response), nil
		}
	}
}

func (m ReplicatedMapImpl) ContainsValue(value interface{}) (bool, error) {
	if valueData, err := m.ValidateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeReplicatedMapContainsValueRequest(m.Name, valueData)
		if response, err := m.InvokeOnPartition(request, m.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeReplicatedMapContainsValueResponse(response), nil
		}
	}
}

func (m ReplicatedMapImpl) Get(key interface{}) (interface{}, error) {
	if keyData, err := m.ValidateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeReplicatedMapGetRequest(m.Name, keyData)
		if response, err := m.InvokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return m.ConvertToObject(codec.DecodeReplicatedMapGetResponse(response))
		}
	}
}

func (m ReplicatedMapImpl) GetEntrySet() ([]hztypes.Entry, error) {
	request := codec.EncodeReplicatedMapEntrySetRequest(m.Name)
	if response, err := m.InvokeOnPartition(request, m.partitionID); err != nil {
		return nil, err
	} else {
		return m.ConvertPairsToEntries(codec.DecodeReplicatedMapEntrySetResponse(response))
	}
}

func (m ReplicatedMapImpl) GetKeySet() ([]interface{}, error) {
	request := codec.EncodeReplicatedMapKeySetRequest(m.Name)
	if response, err := m.InvokeOnPartition(request, m.partitionID); err != nil {
		return nil, err
	} else {
		keyDatas := codec.DecodeReplicatedMapKeySetResponse(response)
		keys := make([]interface{}, len(keyDatas))
		for i, keyData := range keyDatas {
			if key, err := m.ConvertToObject(keyData); err != nil {
				return nil, err
			} else {
				keys[i] = key
			}
		}
		return keys, nil
	}
}

func (m ReplicatedMapImpl) GetValues() ([]interface{}, error) {
	request := codec.EncodeReplicatedMapValuesRequest(m.Name)
	if response, err := m.InvokeOnPartition(request, m.partitionID); err != nil {
		return nil, err
	} else {
		valueDatas := codec.DecodeReplicatedMapValuesResponse(response)
		values := make([]interface{}, len(valueDatas))
		for i, valueData := range valueDatas {
			if value, err := m.ConvertToObject(valueData); err != nil {
				return nil, err
			} else {
				values[i] = value
			}
		}
		return values, nil
	}
}

func (m ReplicatedMapImpl) IsEmpty() (bool, error) {
	request := codec.EncodeReplicatedMapIsEmptyRequest(m.Name)
	if response, err := m.InvokeOnPartition(request, m.partitionID); err != nil {
		return false, err
	} else {
		return codec.DecodeReplicatedMapIsEmptyResponse(response), nil
	}
}

func (m ReplicatedMapImpl) ListenEntryNotification(subscriptionID int, handler hztypes.EntryNotifiedHandler) error {
	return m.listenEntryNotified(nil, nil, subscriptionID, handler)
}

func (m ReplicatedMapImpl) ListenEntryNotificationToKey(key interface{}, subscriptionID int, handler hztypes.EntryNotifiedHandler) error {
	return m.listenEntryNotified(key, nil, subscriptionID, handler)
}

func (m ReplicatedMapImpl) ListenEntryNotificationWithPredicate(predicate predicate.Predicate, subscriptionID int, handler hztypes.EntryNotifiedHandler) error {
	return m.listenEntryNotified(nil, predicate, subscriptionID, handler)
}

func (m ReplicatedMapImpl) ListenEntryNotificationToKeyWithPredicate(key interface{}, predicate predicate.Predicate, subscriptionID int, handler hztypes.EntryNotifiedHandler) error {
	return m.listenEntryNotified(key, predicate, subscriptionID, handler)
}

func (m ReplicatedMapImpl) Put(key interface{}, value interface{}) (interface{}, error) {
	if keyData, valueData, err := m.ValidateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeReplicatedMapPutRequest(m.Name, keyData, valueData, ttlUnlimited)
		if response, err := m.InvokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return m.ConvertToObject(codec.DecodeReplicatedMapPutResponse(response))
		}
	}
}

func (m ReplicatedMapImpl) PutAll(keyValuePairs []hztypes.Entry) error {
	if partitionToPairs, err := m.PartitionToPairs(keyValuePairs); err != nil {
		return err
	} else {
		// create invocations
		invs := make([]invocation.Invocation, 0, len(partitionToPairs))
		for partitionID, entries := range partitionToPairs {
			inv := m.InvokeOnPartitionAsync(codec.EncodeReplicatedMapPutAllRequest(m.Name, entries), partitionID)
			invs = append(invs, inv)
		}
		// wait for responses
		for _, inv := range invs {
			if _, err := inv.Get(); err != nil {
				// TODO: prevent leak when some inv.Get()s are not executed due to error of other ones.
				return err
			}
		}
		return nil
	}
}

func (m ReplicatedMapImpl) Remove(key interface{}) (interface{}, error) {
	if keyData, err := m.ValidateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeReplicatedMapRemoveRequest(m.Name, keyData)
		if response, err := m.InvokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return m.ConvertToObject(codec.DecodeReplicatedMapRemoveResponse(response))
		}
	}
}

func (m ReplicatedMapImpl) Size() (int, error) {
	request := codec.EncodeReplicatedMapSizeRequest(m.Name)
	if response, err := m.InvokeOnPartition(request, m.partitionID); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeReplicatedMapSizeResponse(response)), nil
	}
}

func (m ReplicatedMapImpl) UnlistenEntryNotification(subscriptionID int) error {
	m.userEventDispatcher.Unsubscribe(hztypes.EventEntryNotified, subscriptionID)
	return m.listenerBinder.Remove(m.Name, subscriptionID)
}

func (m *ReplicatedMapImpl) listenEntryNotified(key interface{}, predicate predicate.Predicate, subscriptionID int, handler hztypes.EntryNotifiedHandler) error {
	var request *proto.ClientMessage
	var err error
	var keyData pubserialization.Data
	var predicateData pubserialization.Data
	if key != nil {
		if keyData, err = m.ValidateAndSerialize(key); err != nil {
			return err
		}
	}
	if predicate != nil {
		if predicateData, err = m.ValidateAndSerialize(predicate); err != nil {
			return err
		}
	}
	if keyData != nil {
		if predicateData != nil {
			request = codec.EncodeReplicatedMapAddEntryListenerToKeyWithPredicateRequest(m.Name, keyData, predicateData, m.smartRouting)
		} else {
			request = codec.EncodeReplicatedMapAddEntryListenerToKeyRequest(m.Name, keyData, m.smartRouting)
		}
	} else if predicateData != nil {
		request = codec.EncodeReplicatedMapAddEntryListenerWithPredicateRequest(m.Name, predicateData, m.smartRouting)
	} else {
		request = codec.EncodeReplicatedMapAddEntryListenerRequest(m.Name, m.smartRouting)
	}
	err = m.listenerBinder.Add(request, subscriptionID, func(msg *proto.ClientMessage) {
		handler := func(binKey pubserialization.Data, binValue pubserialization.Data, binOldValue pubserialization.Data, binMergingValue pubserialization.Data, binEventType int32, binUUID internal.UUID, numberOfAffectedEntries int32) {
			key := m.MustConvertToInterface(binKey, "invalid key at ListenEntryNotification")
			value := m.MustConvertToInterface(binValue, "invalid value at ListenEntryNotification")
			oldValue := m.MustConvertToInterface(binOldValue, "invalid oldValue at ListenEntryNotification")
			mergingValue := m.MustConvertToInterface(binMergingValue, "invalid mergingValue at ListenEntryNotification")
			m.userEventDispatcher.Publish(newEntryNotifiedEventImpl(m.Name, binUUID.String(), key, value, oldValue, mergingValue, int(numberOfAffectedEntries)))
		}
		if keyData != nil {
			if predicateData != nil {
				codec.HandleReplicatedMapAddEntryListenerToKeyWithPredicate(msg, handler)
			} else {
				codec.HandleReplicatedMapAddEntryListenerToKey(msg, handler)
			}
		} else if predicateData != nil {
			codec.HandleReplicatedMapAddEntryListenerWithPredicate(msg, handler)
		} else {
			codec.HandleReplicatedMapAddEntryListener(msg, handler)
		}
	})
	if err != nil {
		return err
	}
	m.userEventDispatcher.Subscribe(hztypes.EventEntryNotified, subscriptionID, func(event event.Event) {
		if entryNotifiedEvent, ok := event.(*hztypes.EntryNotified); ok {
			if entryNotifiedEvent.OwnerName == m.Name {
				handler(entryNotifiedEvent)
			}
		} else {
			panic("cannot cast event to hztypes.EntryNotified event")
		}
	})
	return nil
}
