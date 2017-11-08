package core

func Sql(sql string) IPredicate {
	return NewSqlPredicate(sql)
}

func And(predicates []IPredicate) IPredicate {
	return NewAndPredicate(predicates)
}
