package event

import "reflect"

// MakeSubscriptionID creates subscription ID from a function
func MakeSubscriptionID(fun interface{}) int {
	value := reflect.ValueOf(fun)
	if value.Kind() != reflect.Func {
		panic("not a func")
	}
	return int(value.Pointer())
}
