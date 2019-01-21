// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package predicate

import (
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type predicate struct {
	id int32
}

func newPredicate(id int32) *predicate {
	return &predicate{id}
}

func (p *predicate) ReadData(input serialization.DataInput) error {
	return nil
}

func (p *predicate) WriteData(output serialization.DataOutput) error {
	return nil
}

func (*predicate) FactoryID() int32 {
	return FactoryID
}

func (p *predicate) ClassID() int32 {
	return p.id
}

type SQL struct {
	*predicate
	sql string
}

func NewSQL(sql string) *SQL {
	return &SQL{newPredicate(sqlID), sql}
}

func (sp *SQL) ReadData(input serialization.DataInput) error {
	var err error
	sp.predicate = newPredicate(sqlID)
	sp.sql, err = input.ReadUTF()
	return err
}

func (sp *SQL) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(sp.sql)
	return nil
}

type And struct {
	*predicate
	predicates []interface{}
}

func NewAnd(predicates []interface{}) *And {
	return &And{newPredicate(andID), predicates}
}

func (ap *And) ReadData(input serialization.DataInput) error {
	ap.predicate = newPredicate(andID)
	length, err := input.ReadInt32()
	ap.predicates = make([]interface{}, length)
	for i := 0; i < int(length); i++ {
		pred, err := input.ReadObject()
		if err != nil {
			return err
		}
		ap.predicates[i] = pred
	}
	return err
}

func (ap *And) WriteData(output serialization.DataOutput) error {
	output.WriteInt32(int32(len(ap.predicates)))
	for _, pred := range ap.predicates {
		err := output.WriteObject(pred)
		if err != nil {
			return err
		}
	}
	return nil
}

type Between struct {
	*predicate
	field string
	from  interface{}
	to    interface{}
}

func NewBetween(field string, from interface{}, to interface{}) *Between {
	return &Between{newPredicate(betweenID), field, from, to}
}

func (bp *Between) ReadData(input serialization.DataInput) error {
	var err error
	bp.predicate = newPredicate(betweenID)
	bp.field, err = input.ReadUTF()
	bp.to, err = input.ReadObject()
	bp.from, err = input.ReadObject()
	return err
}

func (bp *Between) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(bp.field)
	err := output.WriteObject(bp.to)
	if err != nil {
		return err
	}
	return output.WriteObject(bp.from)
}

type Equal struct {
	*predicate
	field string
	value interface{}
}

func NewEqual(field string, value interface{}) *Equal {
	return &Equal{newPredicate(equalID), field, value}
}

func (ep *Equal) ReadData(input serialization.DataInput) error {
	var err error
	ep.predicate = newPredicate(equalID)
	ep.field, err = input.ReadUTF()
	ep.value, err = input.ReadObject()
	return err
}

func (ep *Equal) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(ep.field)
	return output.WriteObject(ep.value)
}

type GreaterLess struct {
	*predicate
	field string
	value interface{}
	equal bool
	less  bool
}

func NewGreaterLess(field string, value interface{}, equal bool, less bool) *GreaterLess {
	return &GreaterLess{newPredicate(greaterlessID), field, value, equal, less}
}

func (glp *GreaterLess) ReadData(input serialization.DataInput) error {
	var err error
	glp.predicate = newPredicate(greaterlessID)
	glp.field, err = input.ReadUTF()
	glp.value, err = input.ReadObject()
	glp.equal, err = input.ReadBool()
	glp.less, err = input.ReadBool()
	return err
}

func (glp *GreaterLess) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(glp.field)
	err := output.WriteObject(glp.value)
	if err != nil {
		return err
	}
	output.WriteBool(glp.equal)
	output.WriteBool(glp.less)
	return nil
}

type Like struct {
	*predicate
	field string
	expr  string
}

func NewLike(field string, expr string) *Like {
	return &Like{newPredicate(likeID), field, expr}
}

func (lp *Like) ReadData(input serialization.DataInput) error {
	var err error
	lp.predicate = newPredicate(likeID)
	lp.field, err = input.ReadUTF()
	lp.expr, err = input.ReadUTF()
	return err
}

func (lp *Like) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(lp.field)
	output.WriteUTF(lp.expr)
	return nil
}

type ILike struct {
	*Like
}

func NewILike(field string, expr string) *ILike {
	return &ILike{&Like{newPredicate(ilikeID), field, expr}}
}

func (ilp *ILike) ReadData(input serialization.DataInput) error {
	var err error
	ilp.Like = &Like{predicate: newPredicate(ilikeID)}
	ilp.field, err = input.ReadUTF()
	ilp.expr, err = input.ReadUTF()
	return err
}

type In struct {
	*predicate
	field  string
	values []interface{}
}

func NewIn(field string, values []interface{}) *In {
	return &In{newPredicate(inID), field, values}
}

func (ip *In) ReadData(input serialization.DataInput) error {
	var err error
	ip.predicate = newPredicate(inID)
	ip.field, err = input.ReadUTF()
	length, err := input.ReadInt32()
	ip.values = make([]interface{}, length)
	for i := int32(0); i < length; i++ {
		ip.values[i], err = input.ReadObject()
		if err != nil {
			return err
		}
	}
	return err
}

func (ip *In) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(ip.field)
	output.WriteInt32(int32(len(ip.values)))
	for _, value := range ip.values {
		err := output.WriteObject(value)
		if err != nil {
			return err
		}
	}
	return nil
}

type InstanceOf struct {
	*predicate
	className string
}

func NewInstanceOf(className string) *InstanceOf {
	return &InstanceOf{newPredicate(instanceOfID), className}
}

func (iop *InstanceOf) ReadData(input serialization.DataInput) error {
	var err error
	iop.predicate = newPredicate(instanceOfID)
	iop.className, err = input.ReadUTF()
	return err
}

func (iop *InstanceOf) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(iop.className)
	return nil
}

type NotEqual struct {
	*Equal
}

func NewNotEqual(field string, value interface{}) *NotEqual {
	return &NotEqual{&Equal{newPredicate(notEqualID), field, value}}
}

func (nep *NotEqual) ReadData(input serialization.DataInput) error {
	var err error
	nep.Equal = &Equal{predicate: newPredicate(notEqualID)}
	nep.field, err = input.ReadUTF()
	nep.value, err = input.ReadObject()

	return err
}

type Not struct {
	*predicate
	pred interface{}
}

func NewNot(pred interface{}) *Not {
	return &Not{newPredicate(notID), pred}
}

func (np *Not) ReadData(input serialization.DataInput) error {
	np.predicate = newPredicate(notID)
	i, err := input.ReadObject()
	np.pred = i.(interface{})
	return err
}

func (np *Not) WriteData(output serialization.DataOutput) error {
	return output.WriteObject(np.pred)
}

type Or struct {
	*predicate
	predicates []interface{}
}

func NewOr(predicates []interface{}) *Or {
	return &Or{newPredicate(orID), predicates}
}

func (or *Or) ReadData(input serialization.DataInput) error {
	var err error
	or.predicate = newPredicate(orID)
	length, err := input.ReadInt32()
	or.predicates = make([]interface{}, length)
	for i := 0; i < int(length); i++ {
		pred, err := input.ReadObject()
		if err != nil {
			return err
		}
		or.predicates[i] = pred.(interface{})
	}
	return err
}

func (or *Or) WriteData(output serialization.DataOutput) error {
	output.WriteInt32(int32(len(or.predicates)))
	for _, pred := range or.predicates {
		err := output.WriteObject(pred)
		if err != nil {
			return err
		}
	}
	return nil
}

type Regex struct {
	*predicate
	field string
	regex string
}

func NewRegex(field string, regex string) *Regex {
	return &Regex{newPredicate(regexID), field, regex}
}

func (rp *Regex) ReadData(input serialization.DataInput) error {
	var err error
	rp.predicate = newPredicate(regexID)
	rp.field, err = input.ReadUTF()
	rp.regex, err = input.ReadUTF()
	return err
}

func (rp *Regex) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(rp.field)
	output.WriteUTF(rp.regex)
	return nil
}

type False struct {
	*predicate
}

func NewFalse() *False {
	return &False{newPredicate(falseID)}
}

func (fp *False) ReadData(input serialization.DataInput) error {
	fp.predicate = newPredicate(falseID)
	return nil
}

func (fp *False) WriteData(output serialization.DataOutput) error {
	//Empty method
	return nil
}

type True struct {
	*predicate
}

func NewTrue() *True {
	return &True{newPredicate(trueID)}
}

func (tp *True) ReadData(input serialization.DataInput) error {
	tp.predicate = newPredicate(trueID)
	return nil
}

func (tp *True) WriteData(output serialization.DataOutput) error {
	//Empty method
	return nil
}
