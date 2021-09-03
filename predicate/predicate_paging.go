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
	comparator        serialization.IdentifiedDataSerializable
	internalPredicate Predicate
	iterationType     IterationType
	anchorList        []types.Entry
	pageSize          int32
	page              int32
}

func Paging(pageSize int32) *predPaging {
	return &predPaging{
		internalPredicate: True(),
		comparator:        nil,
		iterationType:     ENTRY,
		pageSize:          pageSize,
		page:              0,
		anchorList:        []types.Entry{},
	}
}

func PagingWithPredicate(internalPredicate Predicate, pageSize int32) *predPaging {
	return &predPaging{
		internalPredicate: internalPredicate,
		comparator:        nil,
		iterationType:     ENTRY,
		pageSize:          pageSize,
		page:              0,
		anchorList:        []types.Entry{},
	}
}

type IterationType string

const (
	KEY   IterationType = "KEY"
	VALUE               = "VALUE"
	ENTRY               = "ENTRY"
)

func IterationTypeToId(t IterationType) int8 {
	switch t {
	case KEY:
		return 0
	case VALUE:
		return 1
	case ENTRY:
		return 2
	default:
		return -1
	}
}

func (p *predPaging) NextPage() {
	p.page++
}

func (p *predPaging) PrevPage() {
	p.page--
}

func (p *predPaging) SetPage(page int32) {
	p.page = page
}

func (p predPaging) GetPage() int32 {
	return p.page
}

func (p predPaging) AnchorList() []types.Entry {
	return p.anchorList
}

func (p predPaging) FactoryID() int32 {
	return factoryID
}

func (p predPaging) ClassID() int32 {
	return 15
}

func (p *predPaging) SetAnchorList(anchorList []types.Entry) {
	p.anchorList = anchorList
}

func (p predPaging) ReadData(input serialization.DataInput) {
	p.internalPredicate = input.ReadObject().(Predicate)
	p.comparator = input.ReadObject().(serialization.IdentifiedDataSerializable)
	p.page = input.ReadInt32()
	p.pageSize = input.ReadInt32()
	p.iterationType = IterationType(input.ReadByte())
	size := input.ReadInt32()
	p.anchorList = make([]types.Entry, size)
	for i := int32(0); i < size; i++ {
		page := input.ReadInt32()
		key := input.ReadObject()
		value := input.ReadObject()
		anchorEntry := types.Entry{Key: key, Value: value}
		p.anchorList[i] = types.Entry{Key: page, Value: anchorEntry}
	}
}

func (p predPaging) WriteData(output serialization.DataOutput) {
	output.WriteObject(p.internalPredicate)
	output.WriteObject(p.comparator)
	output.WriteInt32(p.page)
	output.WriteInt32(p.pageSize)
	output.WriteString(string(p.iterationType))
	output.WriteInt32(int32(len(p.anchorList)))
	for _, a := range p.anchorList {
		output.WriteInt32(a.Key.(int32))
		anchorEntry := a.Value.(types.Entry)
		output.WriteObject(anchorEntry.Key)
		output.WriteObject(anchorEntry.Value)
	}
}

func (p predPaging) String() string {
	return fmt.Sprintf("paging, predicate=%s, pageSize=%d, page=%d",
		p.internalPredicate.String(), p.pageSize, p.page)
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
	p.iterationType = iterationType

	if IsPagingPredicate(p.internalPredicate) {
		return proto.PagingPredicateHolder{}, hzerrors.NewIllegalArgumentError("Nested paging predicates are not supported", nil)
	}

	internalPredicateData, err := toData(p.internalPredicate)
	if err != nil {
		return proto.PagingPredicateHolder{}, err
	}
	comparatorData, err := toData(p.comparator)
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
		byte(IterationTypeToId(p.iterationType)),
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
	p.SetAnchorList(a)
	return nil
}
