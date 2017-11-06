package serialization

import (
	. "github.com/hazelcast/go-client/internal/serialization/api"
)

const PREDICATE_FACTORY_ID = -32

type predicate struct {
	id int32
}

func newPredicate(id int32) *predicate {
	return &predicate{id}
}

func (sp *predicate) ReadData(input DataInput) error {
	return nil
}

func (sp *predicate) WriteData(output DataOutput) {
}

func (*predicate) FactoryId() int32 {
	return PREDICATE_FACTORY_ID
}

func (p *predicate) ClassId() int32 {
	return p.id
}

type SqlPredicate struct {
	*predicate
	sql string
}

func NewSqlPredicate(sql string) *SqlPredicate {
	return &SqlPredicate{newPredicate(SQL_PREDICATE), sql}
}

func (sp *SqlPredicate) ReadData(input DataInput) error {
	var err error
	sp.sql, err = input.ReadUTF()
	return err
}

func (sp *SqlPredicate) WriteData(output DataOutput) {
	output.WriteUTF(sp.sql)
}

type AndPredicate struct {
	*predicate
	predicates []IPredicate
}

func NewAndPredicate(predicates []IPredicate) *AndPredicate {
	return &AndPredicate{newPredicate(AND_PREDICATE), predicates}
}

func (ap *AndPredicate) ReadData(input DataInput) error {
	length, err := input.ReadInt32()
	if err != nil {
		return err
	}
	ap.predicates = make([]IPredicate, 0)
	for i := 0; i < int(length); i++ {
		pred, err := input.ReadObject()
		if err != nil {
			return err
		}
		ap.predicates[i] = pred.(IPredicate)
	}
	return nil
}

func (ap *AndPredicate) WriteData(output DataOutput) {
	output.WriteInt32(int32(len(ap.predicates)))
	for _, pred := range ap.predicates {
		output.WriteObject(pred)
	}
}
