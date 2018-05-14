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

type SQLPredicate struct {
	*predicate
	sql string
}

func NewSQLPredicate(sql string) *SQLPredicate {
	return &SQLPredicate{newPredicate(sqlPredicateID), sql}
}

func (sp *SQLPredicate) ReadData(input serialization.DataInput) error {
	var err error
	sp.predicate = newPredicate(sqlPredicateID)
	sp.sql, err = input.ReadUTF()
	return err
}

func (sp *SQLPredicate) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(sp.sql)
	return nil
}

type AndPredicate struct {
	*predicate
	predicates []interface{}
}

func NewAndPredicate(predicates []interface{}) *AndPredicate {
	return &AndPredicate{newPredicate(andPredicateID), predicates}
}

func (ap *AndPredicate) ReadData(input serialization.DataInput) error {
	ap.predicate = newPredicate(andPredicateID)
	length, err := input.ReadInt32()
	if err != nil {
		return err
	}
	ap.predicates = make([]interface{}, length)
	for i := 0; i < int(length); i++ {
		pred, err := input.ReadObject()
		if err != nil {
			return err
		}
		ap.predicates[i] = pred
	}
	return nil
}

func (ap *AndPredicate) WriteData(output serialization.DataOutput) error {
	output.WriteInt32(int32(len(ap.predicates)))
	for _, pred := range ap.predicates {
		err := output.WriteObject(pred)
		if err != nil {
			return err
		}
	}
	return nil
}

type BetweenPredicate struct {
	*predicate
	field string
	from  interface{}
	to    interface{}
}

func NewBetweenPredicate(field string, from interface{}, to interface{}) *BetweenPredicate {
	return &BetweenPredicate{newPredicate(betweenPredicateID), field, from, to}
}

func (bp *BetweenPredicate) ReadData(input serialization.DataInput) error {
	var err error
	bp.predicate = newPredicate(betweenPredicateID)
	bp.field, err = input.ReadUTF()
	if err != nil {
		return err
	}
	bp.to, err = input.ReadObject()
	if err != nil {
		return err
	}
	bp.from, err = input.ReadObject()

	return err
}

func (bp *BetweenPredicate) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(bp.field)
	err := output.WriteObject(bp.to)
	if err != nil {
		return err
	}
	return output.WriteObject(bp.from)
}

type EqualPredicate struct {
	*predicate
	field string
	value interface{}
}

func NewEqualPredicate(field string, value interface{}) *EqualPredicate {
	return &EqualPredicate{newPredicate(equalPredicateID), field, value}
}

func (ep *EqualPredicate) ReadData(input serialization.DataInput) error {
	var err error
	ep.predicate = newPredicate(equalPredicateID)
	ep.field, err = input.ReadUTF()
	if err != nil {
		return err
	}
	ep.value, err = input.ReadObject()

	return err
}

func (ep *EqualPredicate) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(ep.field)
	return output.WriteObject(ep.value)
}

type GreaterLessPredicate struct {
	*predicate
	field string
	value interface{}
	equal bool
	less  bool
}

func NewGreaterLessPredicate(field string, value interface{}, equal bool, less bool) *GreaterLessPredicate {
	return &GreaterLessPredicate{newPredicate(greaterlessPredicateID), field, value, equal, less}
}

func (glp *GreaterLessPredicate) ReadData(input serialization.DataInput) error {
	var err error
	glp.predicate = newPredicate(greaterlessPredicateID)
	glp.field, err = input.ReadUTF()
	if err != nil {
		return err
	}
	glp.value, err = input.ReadObject()
	if err != nil {
		return err
	}
	glp.equal, err = input.ReadBool()
	if err != nil {
		return err
	}
	glp.less, err = input.ReadBool()
	return err
}

func (glp *GreaterLessPredicate) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(glp.field)
	err := output.WriteObject(glp.value)
	if err != nil {
		return err
	}
	output.WriteBool(glp.equal)
	output.WriteBool(glp.less)
	return nil
}

type LikePredicate struct {
	*predicate
	field string
	expr  string
}

func NewLikePredicate(field string, expr string) *LikePredicate {
	return &LikePredicate{newPredicate(likePredicateID), field, expr}
}

func (lp *LikePredicate) ReadData(input serialization.DataInput) error {
	var err error
	lp.predicate = newPredicate(likePredicateID)
	lp.field, err = input.ReadUTF()
	if err != nil {
		return err
	}
	lp.expr, err = input.ReadUTF()
	return err
}

func (lp *LikePredicate) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(lp.field)
	output.WriteUTF(lp.expr)
	return nil
}

type ILikePredicate struct {
	*LikePredicate
}

func NewILikePredicate(field string, expr string) *ILikePredicate {
	return &ILikePredicate{&LikePredicate{newPredicate(ilikePredicateID), field, expr}}
}

func (ilp *ILikePredicate) ReadData(input serialization.DataInput) error {
	var err error
	ilp.LikePredicate = &LikePredicate{predicate: newPredicate(ilikePredicateID)}
	ilp.field, err = input.ReadUTF()
	if err != nil {
		return err
	}
	ilp.expr, err = input.ReadUTF()
	return err
}

type InPredicate struct {
	*predicate
	field  string
	values []interface{}
}

func NewInPredicate(field string, values []interface{}) *InPredicate {
	return &InPredicate{newPredicate(inPredicateID), field, values}
}

func (ip *InPredicate) ReadData(input serialization.DataInput) error {
	var err error
	ip.predicate = newPredicate(inPredicateID)
	ip.field, err = input.ReadUTF()
	if err != nil {
		return err
	}
	length, err := input.ReadInt32()
	if err != nil {
		return err
	}
	ip.values = make([]interface{}, length)
	for i := int32(0); i < length; i++ {
		ip.values[i], err = input.ReadObject()
		if err != nil {
			return err
		}
	}
	return nil
}

func (ip *InPredicate) WriteData(output serialization.DataOutput) error {
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

type InstanceOfPredicate struct {
	*predicate
	className string
}

func NewInstanceOfPredicate(className string) *InstanceOfPredicate {
	return &InstanceOfPredicate{newPredicate(instanceOfPredicateID), className}
}

func (iop *InstanceOfPredicate) ReadData(input serialization.DataInput) error {
	var err error
	iop.predicate = newPredicate(instanceOfPredicateID)
	iop.className, err = input.ReadUTF()
	return err
}

func (iop *InstanceOfPredicate) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(iop.className)
	return nil
}

type NotEqualPredicate struct {
	*EqualPredicate
}

func NewNotEqualPredicate(field string, value interface{}) *NotEqualPredicate {
	return &NotEqualPredicate{&EqualPredicate{newPredicate(notEqualPredicateID), field, value}}
}

func (nep *NotEqualPredicate) ReadData(input serialization.DataInput) error {
	var err error
	nep.EqualPredicate = &EqualPredicate{predicate: newPredicate(notEqualPredicateID)}
	nep.field, err = input.ReadUTF()
	if err != nil {
		return err
	}
	nep.value, err = input.ReadObject()

	return err
}

type NotPredicate struct {
	*predicate
	pred interface{}
}

func NewNotPredicate(pred interface{}) *NotPredicate {
	return &NotPredicate{newPredicate(notPredicateID), pred}
}

func (np *NotPredicate) ReadData(input serialization.DataInput) error {
	np.predicate = newPredicate(notPredicateID)
	i, err := input.ReadObject()
	np.pred = i.(interface{})
	return err
}

func (np *NotPredicate) WriteData(output serialization.DataOutput) error {
	return output.WriteObject(np.pred)
}

type OrPredicate struct {
	*predicate
	predicates []interface{}
}

func NewOrPredicate(predicates []interface{}) *OrPredicate {
	return &OrPredicate{newPredicate(orPredicateID), predicates}
}

func (or *OrPredicate) ReadData(input serialization.DataInput) error {
	var err error
	or.predicate = newPredicate(orPredicateID)
	length, err := input.ReadInt32()
	if err != nil {
		return err
	}
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

func (or *OrPredicate) WriteData(output serialization.DataOutput) error {
	output.WriteInt32(int32(len(or.predicates)))
	for _, pred := range or.predicates {
		err := output.WriteObject(pred)
		if err != nil {
			return err
		}
	}
	return nil
}

type RegexPredicate struct {
	*predicate
	field string
	regex string
}

func NewRegexPredicate(field string, regex string) *RegexPredicate {
	return &RegexPredicate{newPredicate(regexPredicateID), field, regex}
}

func (rp *RegexPredicate) ReadData(input serialization.DataInput) error {
	var err error
	rp.predicate = newPredicate(regexPredicateID)
	rp.field, err = input.ReadUTF()
	if err != nil {
		return err
	}
	rp.regex, err = input.ReadUTF()
	return err
}

func (rp *RegexPredicate) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(rp.field)
	output.WriteUTF(rp.regex)
	return nil
}

type FalsePredicate struct {
	*predicate
}

func NewFalsePredicate() *FalsePredicate {
	return &FalsePredicate{newPredicate(falsePredicateID)}
}

func (fp *FalsePredicate) ReadData(input serialization.DataInput) error {
	fp.predicate = newPredicate(falsePredicateID)
	return nil
}

func (fp *FalsePredicate) WriteData(output serialization.DataOutput) error {
	//Empty method
	return nil
}

type TruePredicate struct {
	*predicate
}

func NewTruePredicate() *TruePredicate {
	return &TruePredicate{newPredicate(truePredicateID)}
}

func (tp *TruePredicate) ReadData(input serialization.DataInput) error {
	tp.predicate = newPredicate(truePredicateID)
	return nil
}

func (tp *TruePredicate) WriteData(output serialization.DataOutput) error {
	//Empty method
	return nil
}
