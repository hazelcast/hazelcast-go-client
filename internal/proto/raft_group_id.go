package proto

type RaftGroupId struct {
	name string
	seed int64
	id   int64
}

func NewRaftGroupId(name string, seed, id int64) RaftGroupId {
	return RaftGroupId{name, seed, id}
}

func (r RaftGroupId) GetName() string {
	return r.name
}

func (r RaftGroupId) GetSeed() int64 {
	return r.seed
}

func (r RaftGroupId) GetId() int64 {
	return r.id
}
