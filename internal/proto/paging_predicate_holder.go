package proto

import "github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"

type PagingPredicateHolder struct {
	anchorDataListHolder AnchorDataListHolder
	predicateData        serialization.Data
	comparatorData       serialization.Data
	pageSize             int32
	page                 int32
	iterationTypeID      byte
	partitionKeyData     serialization.Data
}

func NewPagingPredicateHolder(anchorDataListHolder AnchorDataListHolder, predicateData, comparatorData serialization.Data,
	pageSize, page int32, iterationTypeID byte, partitionKeyData serialization.Data) PagingPredicateHolder {
	return PagingPredicateHolder{anchorDataListHolder, predicateData, comparatorData,
		pageSize, page, iterationTypeID, partitionKeyData}
}

func (p PagingPredicateHolder) AnchorDataListHolder() AnchorDataListHolder {
	return p.anchorDataListHolder
}

func (p PagingPredicateHolder) PredicateData() serialization.Data {
	return p.predicateData
}

func (p PagingPredicateHolder) ComparatorData() serialization.Data {
	return p.comparatorData
}

func (p PagingPredicateHolder) PageSize() int32 {
	return p.PageSize()
}

func (p PagingPredicateHolder) Page() int32 {
	return p.page
}

func (p PagingPredicateHolder) IterationTypeId() byte {
	return p.iterationTypeID
}

func (p PagingPredicateHolder) PartitionKeyData() serialization.Data {
	return p.partitionKeyData
}
