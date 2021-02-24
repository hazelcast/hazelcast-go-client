package proto

type RaftGroupId struct {
	name string
	seed int64
	id   int64
}

func NewRaftGroupId(name string, seed, id int64) RaftGroupId {
	return RaftGroupId{name, seed, id}
}

func (r RaftGroupId) Name() string {
	return r.name
}

func (r RaftGroupId) Seed() int64 {
	return r.seed
}

func (r RaftGroupId) Id() int64 {
	return r.id
}
