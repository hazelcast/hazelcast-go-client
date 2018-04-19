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

// Package predicates is a utility package to create predicates.
package predicates

import (
	"github.com/hazelcast/hazelcast-go-client/internal/predicates"
)

// Sql returns a SqlPredicateSql with the given sql.
func Sql(sql string) interface{} {
	return predicates.NewSqlPredicate(sql)
}

// And returns an AndPredicate with the given predicates.
func And(predicate ...interface{}) interface{} {
	return predicates.NewAndPredicate(predicate)
}

// Between returns a BetweenPredicate with the given parameters.
func Between(field string, from interface{}, to interface{}) interface{} {
	return predicates.NewBetweenPredicate(field, from, to)
}

// Equal returns an EqualPredicate with the given field and value.
func Equal(field string, value interface{}) interface{} {
	return predicates.NewEqualPredicate(field, value)
}

// GreaterThan returns a GreaterLessPredicate with the given field and value.
// The returned GreaterLessPredicate behaves like greater than.
func GreaterThan(field string, value interface{}) interface{} {
	return predicates.NewGreaterLessPredicate(field, value, false, false)
}

// GreaterEqual returns a GreaterLessPredicate with the given field and value.
// The returned GreaterLessPredicate behaves like greater equal.
func GreaterEqual(field string, value interface{}) interface{} {
	return predicates.NewGreaterLessPredicate(field, value, true, false)
}

// LessThan returns a GreaterLessPredicate with the given field and value.
// The returned GreaterLessPredicate behaves like less than.
func LessThan(field string, value interface{}) interface{} {
	return predicates.NewGreaterLessPredicate(field, value, false, true)
}

// LessEqual returns a GreaterLessPredicate with the given field and value.
// The returned GreaterLessPredicate behaves like less equal.
func LessEqual(field string, value interface{}) interface{} {
	return predicates.NewGreaterLessPredicate(field, value, true, true)
}

// Like returns a LikePredicate with the given field and expr.
func Like(field string, expr string) interface{} {
	return predicates.NewLikePredicate(field, expr)
}

// ILike returns an ILikePredicate with the given field and expr.
func ILike(field string, expr string) interface{} {
	return predicates.NewILikePredicate(field, expr)
}

// In returns an InPredicate with the given field and values.
func In(field string, values ...interface{}) interface{} {
	return predicates.NewInPredicate(field, values)
}

// InstanceOf returns an InstanceOfPredicate with the given className.
func InstanceOf(className string) interface{} {
	return predicates.NewInstanceOfPredicate(className)
}

// NotEqual returns a NotEqualPredicate with the given field and value.
func NotEqual(field string, value interface{}) interface{} {
	return predicates.NewNotEqualPredicate(field, value)
}

// Not returns a NotPredicate with the given predicate.
func Not(predicate interface{}) interface{} {
	return predicates.NewNotPredicate(predicate)
}

// Or returns an OrPredicate with the given predicates.
func Or(predicate ...interface{}) interface{} {
	return predicates.NewOrPredicate(predicate)
}

// Regex returns a RegexPredicate with the given field and regex.
func Regex(field string, regex string) interface{} {
	return predicates.NewRegexPredicate(field, regex)
}

// True returns a TruePredicate.
func True() interface{} {
	return predicates.NewTruePredicate()
}

// False returns a FalsePredicate.
func False() interface{} {
	return predicates.NewFalsePredicate()
}
