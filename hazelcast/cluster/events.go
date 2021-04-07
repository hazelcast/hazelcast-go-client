package cluster

type MemberState int

const (
	MemberStateAdded MemberState = iota
	MemberStateRemoved
)

type MemberStateChangedHandler func(event MemberStateChanged)

const EventMemberStateChanged = "internal.cluster.memberstatechanged"

type MemberStateChanged struct {
	State  MemberState
	Member Member
}

func (e *MemberStateChanged) EventName() string {
	return EventMemberStateChanged
}
