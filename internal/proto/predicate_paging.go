package proto

import (
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type PagingPredicateHolder struct {
	PartitionKeyData     *serialization.Data
	PredicateData        *serialization.Data
	ComparatorData       *serialization.Data
	AnchorDataListHolder AnchorDataListHolder
	PageSize             int32
	Page                 int32
	IterationTypeId      byte
}

func NewPagingPredicateHolder(a AnchorDataListHolder, pred *serialization.Data, c *serialization.Data, ps int32, p int32, i byte, pkd *serialization.Data) PagingPredicateHolder {
	return PagingPredicateHolder{
		AnchorDataListHolder: a,
		PredicateData:        pred,
		ComparatorData:       c,
		PageSize:             ps,
		Page:                 p,
		IterationTypeId:      i,
		PartitionKeyData:     pkd,
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
