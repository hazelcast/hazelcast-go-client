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

package predicates

import (
	"github.com/hazelcast/hazelcast-go-client/internal/predicates"
)

// Sql is a helper function for creating SqlPredicate.
func Sql(sql string) interface{} {
	return predicates.NewSqlPredicate(sql)
}

// And is a helper function for creating AndPredicate.
func And(predicate ...interface{}) interface{} {
	return predicates.NewAndPredicate(predicate)
}

// Between is a helper function for creating BetweenPredicate.
func Between(field string, from interface{}, to interface{}) interface{} {
	return predicates.NewBetweenPredicate(field, from, to)
}

// Equal is a helper function for creating EqualPredicate.
func Equal(field string, value interface{}) interface{} {
	return predicates.NewEqualPredicate(field, value)
}

// GreaterThan is a helper function for creating GreaterLessPredicate behaving like greater than.
func GreaterThan(field string, value interface{}) interface{} {
	return predicates.NewGreaterLessPredicate(field, value, false, false)
}

// GreaterEqual is a helper function for creating GreaterLessPredicate behaving like greater equal.
func GreaterEqual(field string, value interface{}) interface{} {
	return predicates.NewGreaterLessPredicate(field, value, true, false)
}

// LessThan is a helper function for creating GreaterLessPredicate behaving like less than.
func LessThan(field string, value interface{}) interface{} {
	return predicates.NewGreaterLessPredicate(field, value, false, true)
}

// LessEqual is a helper function for creating GreaterLessPredicate behaving like less equal.
func LessEqual(field string, value interface{}) interface{} {
	return predicates.NewGreaterLessPredicate(field, value, true, true)
}

// Like is a helper function for creating LikePredicate.
func Like(field string, expr string) interface{} {
	return predicates.NewLikePredicate(field, expr)
}

// ILike is a helper function for creating ILikePredicate.
func ILike(field string, expr string) interface{} {
	return predicates.NewILikePredicate(field, expr)
}

// In is a helper function for creating InPredicate.
func In(field string, values ...interface{}) interface{} {
	return predicates.NewInPredicate(field, values)
}

// InstanceOf is a helper function for creating InstanceOfPredicate.
func InstanceOf(className string) interface{} {
	return predicates.NewInstanceOfPredicate(className)
}

// NotEqual is a helper function for creating NotEqualPredicate.
func NotEqual(field string, value interface{}) interface{} {
	return predicates.NewNotEqualPredicate(field, value)
}

// Not is a helper function for creating NotPredicate.
func Not(predicate interface{}) interface{} {
	return predicates.NewNotPredicate(predicate)
}

// Or is a helper function for creating OrPredicate.
func Or(predicate ...interface{}) interface{} {
	return predicates.NewOrPredicate(predicate)
}

// Regex is a helper function for creating RegexPredicate.
func Regex(field string, regex string) interface{} {
	return predicates.NewRegexPredicate(field, regex)
}

// True is a helper function for creating TruePredicate.
func True() interface{} {
	return predicates.NewTruePredicate()
}

// False is a helper function for creating FalsePredicate.
func False() interface{} {
	return predicates.NewFalsePredicate()
}
