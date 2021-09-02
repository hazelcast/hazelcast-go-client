package proto

import (
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type PagingPredicateHolder struct {
	AnchorDataListHolder AnchorDataListHolder
	PredicateData        *serialization.Data
	ComparatorData       *serialization.Data
	PageSize             int32
	Page                 int32
	IterationTypeId      byte
	PartitionKeyData     *serialization.Data
}

func NewPagingPredicateHolder(
	anchorDataListHolder AnchorDataListHolder,
	predicateData *serialization.Data,
	comparatorData *serialization.Data,
	pageSize int32,
	page int32,
	iterationTypeId byte,
	partitionKeyData *serialization.Data) PagingPredicateHolder {

	return PagingPredicateHolder{
		AnchorDataListHolder: anchorDataListHolder,
		PredicateData:        predicateData,
		ComparatorData:       comparatorData,
		PageSize:             pageSize,
		Page:                 page,
		IterationTypeId:      iterationTypeId,
		PartitionKeyData:     partitionKeyData,
	}
}

type AnchorDataListHolder struct {
	AnchorPageList []int32
	AnchorDataList []Pair
}

func NewAnchorDataListHolder(anchorPageList []int32, anchorDataList []Pair) AnchorDataListHolder {
	return AnchorDataListHolder{
		AnchorPageList: anchorPageList,
		AnchorDataList: anchorDataList,
	}
}
