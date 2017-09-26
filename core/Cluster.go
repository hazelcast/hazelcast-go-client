package core

type Cluster interface {
	AddListener(listener interface{}) *string
	RemoveListener(registrationId *string) bool
	GetMemberList() []IMember
}
