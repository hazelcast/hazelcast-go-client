package cluster

type MembershipState int

const (
	MembershipStateAdded MembershipState = iota
	MembershipStateRemoved
)

type MembershipStateChangedHandler func(event MembershipStateChanged)

type MembershipStateChanged struct {
	State  MembershipState
	Member Member
}

func (e *MembershipStateChanged) EventName() string {
	return "internal.cluster.membershipstatechanged"
}
