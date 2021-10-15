package check

import "reflect"

/*
Nil does proper nil check for interface{} values taken from users.
See: https://golang.org/doc/faq#nil_error
*/
func Nil(arg interface{}) bool {
	if arg == nil {
		return true
	}
	// TODO: find ways to remove this check, we shouldn't rely on reflection to serialize every single value.
	value := reflect.ValueOf(arg)
	return value.Kind() == reflect.Ptr && value.IsNil()
}
