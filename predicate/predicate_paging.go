package predicate

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/util/nilutil"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type predPaging struct {
	cmp      serialization.IdentifiedDataSerializable
	pred     Predicate
	iterType IterationType
	anchors  []types.Entry
	pageSize int32
	page     int32
}

// Paging creates a paging predicate with a given page size.
func Paging(pageSize int32) *predPaging {
	return &predPaging{
		pred:     True(),
		cmp:      nil,
		iterType: IterationTypeEntry,
		pageSize: pageSize,
		page:     0,
		anchors:  []types.Entry{},
	}
}

// PagingWithPredicate creates a paging predicate with a given page size and a given inner predicate.
func PagingWithPredicate(internalPredicate Predicate, pageSize int32) *predPaging {
	return &predPaging{
		pred:     internalPredicate,
		cmp:      nil,
		iterType: IterationTypeEntry,
		pageSize: pageSize,
		page:     0,
		anchors:  []types.Entry{},
	}
}

// IterationType allows the paging predicate to be used to fetch values, a keyset or an entryset.
type IterationType int8

const (
	IterationTypeKey IterationType = iota
	IterationTypeValue
	IterationTypeEntry
)

// NextPage sets the page of the paging predicate to the next page to fetch the next results in the following operation.
func (p *predPaging) NextPage() {
	p.page++
}

// PrevPage sets the page of the paging predicate to the previous page to fetch the previous results in the following operation.
func (p *predPaging) PrevPage() error {
	if p.page > 0 {
		p.page--
		return nil
	}
	return hzerrors.NewIllegalArgumentError("page can not be negative", nil)
}

// SetPage sets the page to a given page number to fetch the results at that page in the following operation.
func (p *predPaging) SetPage(page int) error {
	if page > 2147483647 {
		return hzerrors.NewIllegalArgumentError("int too big, cannot be cast to int32", nil)
	}
	if page >= 0 {
		p.page = int32(page)
		return nil
	}
	return hzerrors.NewIllegalArgumentError("page can not be negative", nil)
}

// GetPage return the current page of the paging predicate.
func (p predPaging) GetPage() int {
	return int(p.page)
}

// AnchorList returns the anchor list of the paging predicate containing the first item of every page fetched with the predicate.
func (p predPaging) AnchorList() []types.Entry {
	return p.anchors
}

func (p predPaging) FactoryID() int32 {
	return factoryID
}

func (p predPaging) ClassID() int32 {
	return 15
}

func (p predPaging) ReadData(input serialization.DataInput) {
	// This method is not used as the actual type that is (de)serialized is the proto.PagingPredicateHolder
}

func (p predPaging) WriteData(output serialization.DataOutput) {
	// This method is not used as the actual type that is (de)serialized is the proto.PagingPredicateHolder
}

func (p predPaging) String() string {
	return fmt.Sprintf("Paging(predicate=%s, pageSize=%d, page=%d)",
		p.pred.String(), p.pageSize, p.page)
}

func ValidateAndSerializePagingPredicate(
	pred Predicate,
	iterationType IterationType,
	toData func(interface{}) (*iserialization.Data, error)) (proto.PagingPredicateHolder, error) {

	if nilutil.IsNil(pred) {
		return proto.PagingPredicateHolder{}, hzerrors.NewIllegalArgumentError("nil arg is not allowed", nil)
	}

	p, ok := pred.(*predPaging)
	if !ok {
		return proto.PagingPredicateHolder{}, hzerrors.NewSerializationError("Non-paging predicate found unexpectedly, use IsPagingPredicate() to check for predicate", nil)
	}
	p.iterType = iterationType

	if IsPagingPredicate(p.pred) {
		return proto.PagingPredicateHolder{}, hzerrors.NewIllegalArgumentError("Nested paging predicates are not supported", nil)
	}

	internalPredicateData, err := toData(p.pred)
	if err != nil {
		return proto.PagingPredicateHolder{}, err
	}
	comparatorData, err := toData(p.cmp)
	if err != nil {
		return proto.PagingPredicateHolder{}, err
	}
	anchorList, err := serializeAnchorList(p.AnchorList(), toData)
	if err != nil {
		return proto.PagingPredicateHolder{}, err
	}

	return proto.NewPagingPredicateHolder(
		anchorList,
		internalPredicateData,
		comparatorData,
		p.pageSize,
		p.page,
		byte(p.iterType),
		nil), nil
}

func serializeAnchorList(
	a []types.Entry,
	toData func(interface{}) (*iserialization.Data, error)) (proto.AnchorDataListHolder, error) {

	pageList := make([]int32, len(a))
	dataList := make([]proto.Pair, len(a))
	for i := range a {
		pageList[i] = a[i].Key.(int32)
		v := a[i].Value.(types.Entry)
		key, err := toData(v.Key)
		if err != nil {
			return proto.AnchorDataListHolder{}, err
		}
		val, err := toData(v.Value)
		if err != nil {
			return proto.AnchorDataListHolder{}, err
		}
		dataList[i] = proto.NewPair(key, val)
	}
	return proto.AnchorDataListHolder{
		AnchorPageList: pageList,
		AnchorDataList: dataList,
	}, nil
}

func deserializeAnchorList(
	a proto.AnchorDataListHolder,
	toObject func(data *iserialization.Data) (interface{}, error)) ([]types.Entry, error) {

	result := make([]types.Entry, len(a.AnchorPageList))
	for i := range a.AnchorPageList {
		keyBytes, ok := a.AnchorDataList[i].Key().(*iserialization.Data)
		if !ok {
			return nil, hzerrors.NewSerializationError("Unexpected error while deserializing anchor list", nil)
		}
		key, err := toObject(keyBytes)
		if err != nil {
			return nil, err
		}
		valBytes, ok := a.AnchorDataList[i].Value().(*iserialization.Data)
		if !ok {
			return nil, hzerrors.NewSerializationError("Unexpected error while deserializing anchor list", nil)
		}
		val, err := toObject(valBytes)
		if err != nil {
			return nil, err
		}

		result[i] = types.Entry{
			Key: a.AnchorPageList[i],
			Value: types.Entry{
				Key:   key,
				Value: val,
			},
		}
	}
	return result, nil
}

func IsPagingPredicate(pred Predicate) bool {
	_, ok := pred.(*predPaging)
	return ok
}

func UpdateAnchorList(
	pred Predicate,
	updatedAnchorList proto.AnchorDataListHolder,
	toObject func(data *iserialization.Data) (interface{}, error)) error {

	p, ok := pred.(*predPaging)
	if !ok {
		return hzerrors.NewSerializationError("Non-paging predicate found unexpectedly, use IsPagingPredicate() to check for predicate", nil)
	}
	a, err := deserializeAnchorList(updatedAnchorList, toObject)
	if err != nil {
		return err
	}
	p.anchors = a
	return nil
}
