package core

import (
	. "github.com/hazelcast/go-client/internal/predicates"
	. "github.com/hazelcast/go-client/serialization"
)

// Sql is a helper function for creating SqlPredicate.
func Sql(sql string) IPredicate {
	return NewSqlPredicate(sql)
}

// And is a helper function for creating AndPredicate.
func And(predicates []IPredicate) IPredicate {
	return NewAndPredicate(predicates)
}

// Between is a helper function for creating BetweenPredicate.
func Between(field string, from interface{}, to interface{}) IPredicate {
	return NewBetweenPredicate(field, from, to)
}

// Equal is a helper function for creating EqualPredicate.
func Equal(field string, value interface{}) IPredicate {
	return NewEqualPredicate(field, value)
}

// GreaterThan is a helper function for creating GreaterLessPredicate behaving like greater than.
func GreaterThan(field string, value interface{}) IPredicate {
	return NewGreaterLessPredicate(field, value, false, false)
}

// GreaterEqual is a helper function for creating GreaterLessPredicate behaving like greater equal.
func GreaterEqual(field string, value interface{}) IPredicate {
	return NewGreaterLessPredicate(field, value, true, false)
}

// LessThan is a helper function for creating GreaterLessPredicate behaving like less than.
func LessThan(field string, value interface{}) IPredicate {
	return NewGreaterLessPredicate(field, value, false, true)
}

// LessEqual is a helper function for creating GreaterLessPredicate behaving like less equal.
func LessEqual(field string, value interface{}) IPredicate {
	return NewGreaterLessPredicate(field, value, true, true)
}

// Like is a helper function for creating LikePredicate.
func Like(field string, expr string) IPredicate {
	return NewLikePredicate(field, expr)
}

// ILike is a helper function for creating ILikePredicate.
func ILike(field string, expr string) IPredicate {
	return NewILikePredicate(field, expr)
}

// In is a helper function for creating InPredicate.
func In(field string, values []interface{}) IPredicate {
	return NewInPredicate(field, values)
}

// InstanceOf is a helper function for creating InstanceOfPredicate.
func InstanceOf(className string) IPredicate {
	return NewInstanceOfPredicate(className)
}

// NotEqual is a helper function for creating NotEqualPredicate.
func NotEqual(field string, value interface{}) IPredicate {
	return NewNotEqualPredicate(field, value)
}

// Not is a helper function for creating NotPredicate.
func Not(predicate IPredicate) IPredicate {
	return NewNotPredicate(predicate)
}

// Or is a helper function for creating OrPredicate.
func Or(predicates []IPredicate) IPredicate {
	return NewOrPredicate(predicates)
}

// Regex is a helper function for creating RegexPredicate.
func Regex(field string, regex string) IPredicate {
	return NewRegexPredicate(field, regex)
}

// True is a helper function for creating TruePredicate.
func True() IPredicate {
	return NewTruePredicate()
}

// False is a helper function for creating FalsePredicate.
func False() IPredicate {
	return NewFalsePredicate()
}
