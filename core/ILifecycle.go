package core

// ILifecycle is a lifecycle service for Hazelcast clients.
type ILifecycle interface {
	// AddListener adds a listener object to listen for lifecycle events.
	// AddListener returns the registrationId.
	AddListener(listener interface{}) string

	// RemoveListener removes lifecycle listener with the given registrationId.
	// RemoveListener returns true if the listener is removed successfully, false otherwise.
	RemoveListener(registrationId *string) bool
}
