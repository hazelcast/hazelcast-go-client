// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package predicates

import (
	. "github.com/hazelcast/go-client/serialization"
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

func (sp *predicate) WriteData(output DataOutput) error {
	return nil
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
	sp.predicate = newPredicate(SQL_PREDICATE)
	sp.sql, err = input.ReadUTF()
	return err
}

func (sp *SqlPredicate) WriteData(output DataOutput) error {
	output.WriteUTF(sp.sql)
	return nil
}

type AndPredicate struct {
	*predicate
	predicates []IPredicate
}

func NewAndPredicate(predicates []IPredicate) *AndPredicate {
	return &AndPredicate{newPredicate(AND_PREDICATE), predicates}
}

func (ap *AndPredicate) ReadData(input DataInput) error {
	ap.predicate = newPredicate(AND_PREDICATE)
	length, err := input.ReadInt32()
	if err != nil {
		return err
	}
	ap.predicates = make([]IPredicate, length)
	for i := 0; i < int(length); i++ {
		pred, err := input.ReadObject()
		if err != nil {
			return err
		}
		ap.predicates[i] = pred.(IPredicate)
	}
	return nil
}

func (ap *AndPredicate) WriteData(output DataOutput) error {
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
	return &BetweenPredicate{newPredicate(BETWEEN_PREDICATE), field, from, to}
}

func (bp *BetweenPredicate) ReadData(input DataInput) error {
	var err error
	bp.predicate = newPredicate(BETWEEN_PREDICATE)
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

func (bp *BetweenPredicate) WriteData(output DataOutput) error {
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
	return &EqualPredicate{newPredicate(EQUAL_PREDICATE), field, value}
}

func (ep *EqualPredicate) ReadData(input DataInput) error {
	var err error
	ep.predicate = newPredicate(EQUAL_PREDICATE)
	ep.field, err = input.ReadUTF()
	if err != nil {
		return err
	}
	ep.value, err = input.ReadObject()

	return err
}

func (ep *EqualPredicate) WriteData(output DataOutput) error {
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
	return &GreaterLessPredicate{newPredicate(GREATERLESS_PREDICATE), field, value, equal, less}
}

func (glp *GreaterLessPredicate) ReadData(input DataInput) error {
	var err error
	glp.predicate = newPredicate(GREATERLESS_PREDICATE)
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

func (glp *GreaterLessPredicate) WriteData(output DataOutput) error {
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
	return &LikePredicate{newPredicate(LIKE_PREDICATE), field, expr}
}

func (lp *LikePredicate) ReadData(input DataInput) error {
	var err error
	lp.predicate = newPredicate(LIKE_PREDICATE)
	lp.field, err = input.ReadUTF()
	if err != nil {
		return err
	}
	lp.expr, err = input.ReadUTF()
	return err
}

func (lp *LikePredicate) WriteData(output DataOutput) error {
	output.WriteUTF(lp.field)
	output.WriteUTF(lp.expr)
	return nil
}

type ILikePredicate struct {
	*LikePredicate
}

func NewILikePredicate(field string, expr string) *ILikePredicate {
	return &ILikePredicate{&LikePredicate{newPredicate(ILIKE_PREDICATE), field, expr}}
}

func (ilp *ILikePredicate) ReadData(input DataInput) error {
	var err error
	ilp.LikePredicate = &LikePredicate{predicate: newPredicate(ILIKE_PREDICATE)}
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
	return &InPredicate{newPredicate(IN_PREDICATE), field, values}
}

func (ip *InPredicate) ReadData(input DataInput) error {
	var err error
	ip.predicate = newPredicate(IN_PREDICATE)
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

func (ip *InPredicate) WriteData(output DataOutput) error {
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
	return &InstanceOfPredicate{newPredicate(INSTANCEOF_PREDICATE), className}
}

func (iop *InstanceOfPredicate) ReadData(input DataInput) error {
	var err error
	iop.predicate = newPredicate(INSTANCEOF_PREDICATE)
	iop.className, err = input.ReadUTF()
	return err
}

func (iop *InstanceOfPredicate) WriteData(output DataOutput) error {
	output.WriteUTF(iop.className)
	return nil
}

type NotEqualPredicate struct {
	*EqualPredicate
}

func NewNotEqualPredicate(field string, value interface{}) *NotEqualPredicate {
	return &NotEqualPredicate{&EqualPredicate{newPredicate(NOTEQUAL_PREDICATE), field, value}}
}

func (nep *NotEqualPredicate) ReadData(input DataInput) error {
	var err error
	nep.EqualPredicate = &EqualPredicate{predicate: newPredicate(NOTEQUAL_PREDICATE)}
	nep.field, err = input.ReadUTF()
	if err != nil {
		return err
	}
	nep.value, err = input.ReadObject()

	return err
}

type NotPredicate struct {
	*predicate
	pred IPredicate
}

func NewNotPredicate(pred IPredicate) *NotPredicate {
	return &NotPredicate{newPredicate(NOT_PREDICATE), pred}
}

func (np *NotPredicate) ReadData(input DataInput) error {
	np.predicate = newPredicate(NOT_PREDICATE)
	i, err := input.ReadObject()
	np.pred = i.(IPredicate)
	return err
}

func (np *NotPredicate) WriteData(output DataOutput) error {
	return output.WriteObject(np.pred)
}

type OrPredicate struct {
	*predicate
	predicates []IPredicate
}

func NewOrPredicate(predicates []IPredicate) *OrPredicate {
	return &OrPredicate{newPredicate(OR_PREDICATE), predicates}
}

func (or *OrPredicate) ReadData(input DataInput) error {
	var err error
	or.predicate = newPredicate(OR_PREDICATE)
	length, err := input.ReadInt32()
	if err != nil {
		return err
	}
	or.predicates = make([]IPredicate, length)
	for i := 0; i < int(length); i++ {
		pred, err := input.ReadObject()
		if err != nil {
			return err
		}
		or.predicates[i] = pred.(IPredicate)
	}
	return err
}

func (or *OrPredicate) WriteData(output DataOutput) error {
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
	return &RegexPredicate{newPredicate(REGEX_PREDICATE), field, regex}
}

func (rp *RegexPredicate) ReadData(input DataInput) error {
	var err error
	rp.predicate = newPredicate(REGEX_PREDICATE)
	rp.field, err = input.ReadUTF()
	if err != nil {
		return err
	}
	rp.regex, err = input.ReadUTF()
	return err
}

func (rp *RegexPredicate) WriteData(output DataOutput) error {
	output.WriteUTF(rp.field)
	output.WriteUTF(rp.regex)
	return nil
}

type FalsePredicate struct {
	*predicate
}

func NewFalsePredicate() *FalsePredicate {
	return &FalsePredicate{newPredicate(FALSE_PREDICATE)}
}
func (fp *FalsePredicate) ReadData(input DataInput) error {
	fp.predicate = newPredicate(FALSE_PREDICATE)
	return nil
}

func (fp *FalsePredicate) WriteData(output DataOutput) error {
	//Empty method
	return nil
}

type TruePredicate struct {
	*predicate
}

func NewTruePredicate() *TruePredicate {
	return &TruePredicate{newPredicate(TRUE_PREDICATE)}
}
func (tp *TruePredicate) ReadData(input DataInput) error {
	tp.predicate = newPredicate(TRUE_PREDICATE)
	return nil
}

func (tp *TruePredicate) WriteData(output DataOutput) error {
	//Empty method
	return nil
}
