package serialization

import ."github.com/hazelcast/go-client/internal/serialization/api"

type SqlPredicate struct {
	sql string
}

const PREDICATE_FACTORY_ID = -32

func NewSqlPredicate(sql string) SqlPredicate {
	return SqlPredicate{sql}
}

func (sp *SqlPredicate) ReadData(input DataInput) interface{} {
	sp.sql = input.ReadUTF()
	return nil
}

func (sp *SqlPredicate) WriteData(output DataOutput) {
	output.WriteUTF(sp.sql)
}

func (sp *SqlPredicate) GetFactoryId() int32 {
	return PREDICATE_FACTORY_ID
}

func (*SqlPredicate) GetClassId() int32 {
	return 0
}

type AndPredicate struct {
	predicates []IdentifiedDataSerializable
}

func NewAndPredicate(predicates []IdentifiedDataSerializable) AndPredicate {
	return AndPredicate{predicates}
}

func (ap *AndPredicate) ReadData(input DataInput) interface{} {
	var length int32
	length,_=input.ReadInt32()
	ap.predicates=make([]IdentifiedDataSerializable, 0)
	for i:=0; i<int(length); i++ {
		ap.predicates[i]=input.ReadObject().(IdentifiedDataSerializable)
	}
	return nil
}

func (ap *AndPredicate) WriteData(output DataOutput) {
	output.WriteInt32(int32(len(ap.predicates)))
	for _,pred:=range ap.predicates{
		output.WriteObject(pred)
	}
}

func (ap *AndPredicate) GetFactoryId() int32 {
	return PREDICATE_FACTORY_ID
}

func (*AndPredicate) GetClassId() int32 {
	return 0
}


