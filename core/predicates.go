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

package core

import (
	. "github.com/hazelcast/go-client/internal/predicates"
	. "github.com/hazelcast/go-client/serialization"
)

func Sql(sql string) IPredicate {
	return NewSqlPredicate(sql)
}

func And(predicates []IPredicate) IPredicate {
	return NewAndPredicate(predicates)
}

func Between(field string, from interface{}, to interface{}) IPredicate {
	return NewBetweenPredicate(field, from, to)
}

func Equal(field string, value interface{}) IPredicate {
	return NewEqualPredicate(field, value)
}

func GreaterThan(field string, value interface{}) IPredicate {
	return NewGreaterLessPredicate(field, value, false, false)
}

func GreaterEqual(field string, value interface{}) IPredicate {
	return NewGreaterLessPredicate(field, value, true, false)
}

func LessThan(field string, value interface{}) IPredicate {
	return NewGreaterLessPredicate(field, value, false, true)
}

func LessEqual(field string, value interface{}) IPredicate {
	return NewGreaterLessPredicate(field, value, true, true)
}

func Like(field string, expr string) IPredicate {
	return NewLikePredicate(field, expr)
}

func ILike(field string, expr string) IPredicate {
	return NewILikePredicate(field, expr)
}

func In(field string, values []interface{}) IPredicate {
	return NewInPredicate(field, values)
}

func InstanceOf(className string) IPredicate {
	return NewInstanceOfPredicate(className)
}

func NotEqual(field string, value interface{}) IPredicate {
	return NewNotEqualPredicate(field, value)
}

func Not(predicate IPredicate) IPredicate {
	return NewNotPredicate(predicate)
}

func Or(predicates []IPredicate) IPredicate {
	return NewOrPredicate(predicates)
}

func Regex(field string, regex string) IPredicate {
	return NewRegexPredicate(field, regex)
}

func True() IPredicate {
	return NewTruePredicate()
}

func False() IPredicate {
	return NewFalsePredicate()
}
