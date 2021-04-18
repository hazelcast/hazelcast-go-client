package hazelcast

import (
	"context"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type ReplicatedMap struct {
	*proxy
	referenceIDGenerator iproxy.ReferenceIDGenerator
	partitionID          int32
	ctx                  context.Context
}

func NewReplicatedMapImpl(p *proxy) *ReplicatedMap {
	partitionID, err := p.PartitionService.GetPartitionIDWithKey(p.Name)
	if err != nil {
		panic(fmt.Sprintf("error getting partition id with key: %s", p.Name))
	}
	return &ReplicatedMap{
		proxy:                p,
		referenceIDGenerator: iproxy.NewReferenceIDGeneratorImpl(),
		partitionID:          partitionID,
		ctx:                  context.Background(),
	}
}

func (m *ReplicatedMap) withContext(ctx context.Context) *ReplicatedMap {
	return &ReplicatedMap{
		proxy:                m.proxy,
		referenceIDGenerator: m.referenceIDGenerator,
		partitionID:          m.partitionID,
		ctx:                  ctx,
	}
}

// Clear deletes all entries one by one and fires related events
func (m ReplicatedMap) Clear() error {
	request := codec.EncodeReplicatedMapClearRequest(m.Name)
	_, err := m.InvokeOnRandomTarget(m.ctx, request, nil)
	return err
}

// ContainsKey returns true if the map contains an entry with the given key
func (m ReplicatedMap) ContainsKey(key interface{}) (bool, error) {
	if keyData, err := m.ValidateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeReplicatedMapContainsKeyRequest(m.Name, keyData)
		if response, err := m.InvokeOnKey(m.ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeReplicatedMapContainsKeyResponse(response), nil
		}
	}
}

// ContainsValue returns true if the map contains an entry with the given value
func (m ReplicatedMap) ContainsValue(value interface{}) (bool, error) {
	if valueData, err := m.ValidateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeReplicatedMapContainsValueRequest(m.Name, valueData)
		if response, err := m.InvokeOnPartition(m.ctx, request, m.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeReplicatedMapContainsValueResponse(response), nil
		}
	}
}

// Get returns the value for the specified key, or nil if this map does not contain this key.
// Warning:
//   This method returns a clone of original value, modifying the returned value does not change the
//   actual value in the map. One should put modified value back to make changes visible to all nodes.
func (m ReplicatedMap) Get(key interface{}) (interface{}, error) {
	if keyData, err := m.ValidateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeReplicatedMapGetRequest(m.Name, keyData)
		if response, err := m.InvokeOnKey(m.ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.ConvertToObject(codec.DecodeReplicatedMapGetResponse(response))
		}
	}
}

// GetEntrySet returns a clone of the mappings contained in this map.
func (m ReplicatedMap) GetEntrySet() ([]types.Entry, error) {
	request := codec.EncodeReplicatedMapEntrySetRequest(m.Name)
	if response, err := m.InvokeOnPartition(m.ctx, request, m.partitionID); err != nil {
		return nil, err
	} else {
		return m.ConvertPairsToEntries(codec.DecodeReplicatedMapEntrySetResponse(response))
	}
}

// GetKeySet returns keys contained in this map
func (m ReplicatedMap) GetKeySet() ([]interface{}, error) {
	request := codec.EncodeReplicatedMapKeySetRequest(m.Name)
	if response, err := m.InvokeOnPartition(m.ctx, request, m.partitionID); err != nil {
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

// GetValues returns a list clone of the values contained in this map
func (m ReplicatedMap) GetValues() ([]interface{}, error) {
	request := codec.EncodeReplicatedMapValuesRequest(m.Name)
	if response, err := m.InvokeOnPartition(m.ctx, request, m.partitionID); err != nil {
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

// IsEmpty returns true if this map contains no key-value mappings.
func (m ReplicatedMap) IsEmpty() (bool, error) {
	request := codec.EncodeReplicatedMapIsEmptyRequest(m.Name)
	if response, err := m.InvokeOnPartition(m.ctx, request, m.partitionID); err != nil {
		return false, err
	} else {
		return codec.DecodeReplicatedMapIsEmptyResponse(response), nil
	}
}

// ListenEntryNotification adds a continuous entry listener to this map.
func (m ReplicatedMap) ListenEntryNotification(subscriptionID int, handler EntryNotifiedHandler) error {
	return m.listenEntryNotified(nil, nil, subscriptionID, handler)
}

// ListenEntryNotification adds a continuous entry listener to this map.
func (m ReplicatedMap) ListenEntryNotificationToKey(key interface{}, subscriptionID int, handler EntryNotifiedHandler) error {
	return m.listenEntryNotified(key, nil, subscriptionID, handler)
}

// ListenEntryNotification adds a continuous entry listener to this map.
func (m ReplicatedMap) ListenEntryNotificationWithPredicate(predicate predicate.Predicate, subscriptionID int, handler EntryNotifiedHandler) error {
	return m.listenEntryNotified(nil, predicate, subscriptionID, handler)
}

// ListenEntryNotification adds a continuous entry listener to this map.
func (m ReplicatedMap) ListenEntryNotificationToKeyWithPredicate(key interface{}, predicate predicate.Predicate, subscriptionID int, handler EntryNotifiedHandler) error {
	return m.listenEntryNotified(key, predicate, subscriptionID, handler)
}

// Put sets the value for the given key and returns the old value.
func (m ReplicatedMap) Put(key interface{}, value interface{}) (interface{}, error) {
	if keyData, valueData, err := m.ValidateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeReplicatedMapPutRequest(m.Name, keyData, valueData, TtlUnlimited)
		if response, err := m.InvokeOnKey(m.ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.ConvertToObject(codec.DecodeReplicatedMapPutResponse(response))
		}
	}
}

// PutALl copies all of the mappings from the specified map to this map.
// No atomicity guarantees are given. In the case of a failure, some of the key-value tuples may get written,
// while others are not.
func (m ReplicatedMap) PutAll(keyValuePairs []types.Entry) error {
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

// Remove deletes the value for the given key and returns it.
func (m ReplicatedMap) Remove(key interface{}) (interface{}, error) {
	if keyData, err := m.ValidateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeReplicatedMapRemoveRequest(m.Name, keyData)
		if response, err := m.InvokeOnKey(m.ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.ConvertToObject(codec.DecodeReplicatedMapRemoveResponse(response))
		}
	}
}

// Size returns the number of entries in this map.
func (m ReplicatedMap) Size() (int, error) {
	request := codec.EncodeReplicatedMapSizeRequest(m.Name)
	if response, err := m.InvokeOnPartition(m.ctx, request, m.partitionID); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeReplicatedMapSizeResponse(response)), nil
	}
}

// UnlistenEntryNotification removes the specified entry listener.
func (m ReplicatedMap) UnlistenEntryNotification(subscriptionID int) error {
	m.UserEventDispatcher.Unsubscribe(EventEntryNotified, subscriptionID)
	return m.ListenerBinder.Remove(m.Name, subscriptionID)
}

func (m *ReplicatedMap) listenEntryNotified(key interface{}, predicate predicate.Predicate, subscriptionID int, handler EntryNotifiedHandler) error {
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
			request = codec.EncodeReplicatedMapAddEntryListenerToKeyWithPredicateRequest(m.Name, keyData, predicateData, m.SmartRouting)
		} else {
			request = codec.EncodeReplicatedMapAddEntryListenerToKeyRequest(m.Name, keyData, m.SmartRouting)
		}
	} else if predicateData != nil {
		request = codec.EncodeReplicatedMapAddEntryListenerWithPredicateRequest(m.Name, predicateData, m.SmartRouting)
	} else {
		request = codec.EncodeReplicatedMapAddEntryListenerRequest(m.Name, m.SmartRouting)
	}
	err = m.ListenerBinder.Add(request, subscriptionID, func(msg *proto.ClientMessage) {
		handler := func(binKey pubserialization.Data, binValue pubserialization.Data, binOldValue pubserialization.Data, binMergingValue pubserialization.Data, binEventType int32, binUUID internal.UUID, numberOfAffectedEntries int32) {
			key := m.MustConvertToInterface(binKey, "invalid key at ListenEntryNotification")
			value := m.MustConvertToInterface(binValue, "invalid value at ListenEntryNotification")
			oldValue := m.MustConvertToInterface(binOldValue, "invalid oldValue at ListenEntryNotification")
			mergingValue := m.MustConvertToInterface(binMergingValue, "invalid mergingValue at ListenEntryNotification")
			m.UserEventDispatcher.Publish(newEntryNotifiedEventImpl(m.Name, binUUID.String(), key, value, oldValue, mergingValue, int(numberOfAffectedEntries)))
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
	m.UserEventDispatcher.Subscribe(EventEntryNotified, subscriptionID, func(event event.Event) {
		if entryNotifiedEvent, ok := event.(*EntryNotified); ok {
			if entryNotifiedEvent.OwnerName == m.Name {
				handler(entryNotifiedEvent)
			}
		} else {
			panic("cannot cast event to hztypes.EntryNotified event")
		}
	})
	return nil
}
