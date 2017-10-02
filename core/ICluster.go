package core

type ICluster interface {
	AddListener(listener interface{}) *string
	RemoveListener(registrationId *string) bool
	GetMemberList() []IMember
}
