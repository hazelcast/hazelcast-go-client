package core

import . "github.com/hazelcast/go-client/serialization"

func Sql(sql string) IPredicate {
	return NewSqlPredicate(sql)
}

func And(predicates []IPredicate) IPredicate {
	return NewAndPredicate(predicates)
}
