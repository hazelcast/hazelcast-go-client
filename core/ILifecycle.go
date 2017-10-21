package core

type ILifecycle interface {
	AddListener(listener interface{}) string
	RemoveListener(registrationId *string) bool
}
