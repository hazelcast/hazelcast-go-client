package sql

type Row struct {
}

func (r Row) ValueAtIndex(index int) interface{} {
	return nil
}

func (r Row) ValueWithName(name string) interface{} {
	return nil
}
