package core

import (
	. "github.com/hazelcast/go-client/internal/serialization"
	. "github.com/hazelcast/go-client/internal/serialization/api"
)

func Sql(sql string) IPredicate {
	return NewSqlPredicate(sql)
}

func And(predicates []IPredicate) IPredicate {
	return NewAndPredicate(predicates)
}
