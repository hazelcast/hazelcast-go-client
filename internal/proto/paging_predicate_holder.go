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

func (p PagingPredicateHolder) GetAnchorDataListHolder() AnchorDataListHolder {
	return p.anchorDataListHolder
}

func (p PagingPredicateHolder) GetPredicateData() serialization.Data {
	return p.predicateData
}

func (p PagingPredicateHolder) GetComparatorData() serialization.Data {
	return p.comparatorData
}

func (p PagingPredicateHolder) GetPageSize() int32 {
	return p.GetPageSize()
}

func (p PagingPredicateHolder) GetPage() int32 {
	return p.page
}

func (p PagingPredicateHolder) GetIterationTypeId() byte {
	return p.iterationTypeID
}

func (p PagingPredicateHolder) GetPartitionKeyData() serialization.Data {
	return p.partitionKeyData
}
