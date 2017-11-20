package core

// ICluster is a cluster service for Hazelcast clients.
type ICluster interface {
	// AddListener registers the given listener.
	// AddListener returns UUID which will be used to remove the listener.
	AddListener(listener interface{}) *string

	// RemoveListener removes the listener with the given registrationId.
	// RemoveListener returns true if successfully removed, false otherwise.
	RemoveListener(registrationId *string) bool

	// GetMemberList returns a slice of members.
	GetMemberList() []IMember
}
