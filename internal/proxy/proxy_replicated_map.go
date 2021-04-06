package proxy

import (
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"
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
	request := codec.EncodeReplicatedMapClearRequest(m.name)
	_, err := m.invokeOnRandomTarget(request, nil)
	return err
}

func (m ReplicatedMapImpl) ContainsKey(key interface{}) (bool, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeReplicatedMapContainsKeyRequest(m.name, keyData)
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeReplicatedMapContainsKeyResponse(response), nil
		}
	}
}

func (m ReplicatedMapImpl) ContainsValue(value interface{}) (bool, error) {
	if valueData, err := m.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeReplicatedMapContainsValueRequest(m.name, valueData)
		if response, err := m.invokeOnPartition(request, m.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeReplicatedMapContainsValueResponse(response), nil
		}
	}
}

func (m ReplicatedMapImpl) Get(key interface{}) (interface{}, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeReplicatedMapGetRequest(m.name, keyData)
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeReplicatedMapGetResponse(response))
		}
	}
}

func (m ReplicatedMapImpl) GetEntrySet() ([]hztypes.Entry, error) {
	request := codec.EncodeReplicatedMapEntrySetRequest(m.name)
	if response, err := m.invokeOnPartition(request, m.partitionID); err != nil {
		return nil, err
	} else {
		return m.convertPairsToEntries(codec.DecodeReplicatedMapEntrySetResponse(response))
	}
}

func (m ReplicatedMapImpl) GetKeySet() ([]interface{}, error) {
	request := codec.EncodeReplicatedMapKeySetRequest(m.name)
	if response, err := m.invokeOnPartition(request, m.partitionID); err != nil {
		return nil, err
	} else {
		keyDatas := codec.DecodeReplicatedMapKeySetResponse(response)
		keys := make([]interface{}, len(keyDatas))
		for i, keyData := range keyDatas {
			if key, err := m.convertToObject(keyData); err != nil {
				return nil, err
			} else {
				keys[i] = key
			}
		}
		return keys, nil
	}
}

func (m ReplicatedMapImpl) GetValues() ([]interface{}, error) {
	request := codec.EncodeReplicatedMapValuesRequest(m.name)
	if response, err := m.invokeOnPartition(request, m.partitionID); err != nil {
		return nil, err
	} else {
		valueDatas := codec.DecodeReplicatedMapValuesResponse(response)
		values := make([]interface{}, len(valueDatas))
		for i, valueData := range valueDatas {
			if value, err := m.convertToObject(valueData); err != nil {
				return nil, err
			} else {
				values[i] = value
			}
		}
		return values, nil
	}
}

func (m ReplicatedMapImpl) IsEmpty() (bool, error) {
	request := codec.EncodeReplicatedMapIsEmptyRequest(m.name)
	if response, err := m.invokeOnRandomTarget(request, nil); err != nil {
		return false, err
	} else {
		return codec.DecodeReplicatedMapIsEmptyResponse(response), nil
	}
}

func (m ReplicatedMapImpl) ListenEntryNotification(config hztypes.MapEntryListenerConfig, handler hztypes.EntryNotifiedHandler) error {
	panic("implement me")
}

func (m ReplicatedMapImpl) Put(key interface{}, value interface{}) (interface{}, error) {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeReplicatedMapPutRequest(m.name, keyData, valueData, ttlUnlimited)
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeReplicatedMapPutResponse(response))
		}
	}
}

func (m ReplicatedMapImpl) PutAll(keyValuePairs []hztypes.Entry) error {
	if partitionToPairs, err := m.partitionToPairs(keyValuePairs); err != nil {
		return err
	} else {
		// create invocations
		invs := make([]invocation.Invocation, 0, len(partitionToPairs))
		for partitionID, entries := range partitionToPairs {
			inv := m.invokeOnPartitionAsync(codec.EncodeReplicatedMapPutAllRequest(m.name, entries), partitionID)
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
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeReplicatedMapRemoveRequest(m.name, keyData)
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeReplicatedMapRemoveResponse(response))
		}
	}
}

func (m ReplicatedMapImpl) Size() (int, error) {
	panic("implement me")
}

func (m ReplicatedMapImpl) UnlistenEntryNotification(handler hztypes.EntryNotifiedHandler) error {
	panic("implement me")
}
