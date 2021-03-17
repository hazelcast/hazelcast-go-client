// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

// Package predicate is a utility package to create predicate.
package predicate

import (
	"github.com/hazelcast/hazelcast-go-client/v3/internal/predicate"
)

// SQL returns a SQLPredicate with the given sql.
func SQL(sql string) interface{} {
	return predicate.NewSQL(sql)
}

// And returns an AndPredicate with the given predicate.
func And(predicates ...interface{}) interface{} {
	return predicate.NewAnd(predicates)
}

// Between returns a BetweenPredicate with the given parameters.
func Between(field string, from interface{}, to interface{}) interface{} {
	return predicate.NewBetween(field, from, to)
}

// Equal returns an EqualPredicate with the given field and value.
func Equal(field string, value interface{}) interface{} {
	return predicate.NewEqual(field, value)
}

// GreaterThan returns a GreaterLessPredicate with the given field and value.
// The returned GreaterLessPredicate behaves like greater than.
func GreaterThan(field string, value interface{}) interface{} {
	return predicate.NewGreaterLess(field, value, false, false)
}

// GreaterEqual returns a GreaterLessPredicate with the given field and value.
// The returned GreaterLessPredicate behaves like greater equal.
func GreaterEqual(field string, value interface{}) interface{} {
	return predicate.NewGreaterLess(field, value, true, false)
}

// LessThan returns a GreaterLessPredicate with the given field and value.
// The returned GreaterLessPredicate behaves like less than.
func LessThan(field string, value interface{}) interface{} {
	return predicate.NewGreaterLess(field, value, false, true)
}

// LessEqual returns a GreaterLessPredicate with the given field and value.
// The returned GreaterLessPredicate behaves like less equal.
func LessEqual(field string, value interface{}) interface{} {
	return predicate.NewGreaterLess(field, value, true, true)
}

// Like returns a LikePredicate with the given field and expr.
func Like(field string, expr string) interface{} {
	return predicate.NewLike(field, expr)
}

// ILike returns an ILikePredicate with the given field and expr.
func ILike(field string, expr string) interface{} {
	return predicate.NewILike(field, expr)
}

// In returns an InPredicate with the given field and values.
func In(field string, values ...interface{}) interface{} {
	return predicate.NewIn(field, values)
}

// InstanceOf returns an InstanceOfPredicate with the given className.
func InstanceOf(className string) interface{} {
	return predicate.NewInstanceOf(className)
}

// NotEqual returns a NotEqualPredicate with the given field and value.
func NotEqual(field string, value interface{}) interface{} {
	return predicate.NewNotEqual(field, value)
}

// Not returns a NotPredicate with the given predicate.
func Not(predicates interface{}) interface{} {
	return predicate.NewNot(predicates)
}

// Or returns an OrPredicate with the given predicate.
func Or(predicates ...interface{}) interface{} {
	return predicate.NewOr(predicates)
}

// Regex returns a RegexPredicate with the given field and regex.
func Regex(field string, regex string) interface{} {
	return predicate.NewRegex(field, regex)
}

// True returns a TruePredicate.
func True() interface{} {
	return predicate.NewTrue()
}

// False returns a FalsePredicate.
func False() interface{} {
	return predicate.NewFalse()
}
