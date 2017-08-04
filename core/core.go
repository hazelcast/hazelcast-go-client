package core

import (
	"bytes"
	"reflect"

	. "github.com/hazelcast/go-client/internal/serialization"
)

type Address struct {
	Host string
	Port int
}
type Uuid struct {
	Msb int64
	Lsb int64
}
type Member struct {
	Address      Address
	Uuid         string
	IsLiteMember bool
	Attributes   map[string]string
}
type Pair struct {
	Key, Value interface{}
}

func (member1 *Member) Equal(member2 Member) bool {
	if member1.Address != member2.Address {
		return false
	}
	if member1.Uuid != member2.Uuid {
		return false
	}
	if member1.IsLiteMember != member2.IsLiteMember {
		return false
	}
	if !reflect.DeepEqual(member1.Attributes, member2.Attributes) {
		return false
	}
	return true
}

type DistributedObjectInfo struct {
	Name        string
	ServiceName string
}
type EntryView struct {
	Key                    Data
	Value                  Data
	Cost                   int64
	CreationTime           int64
	ExpirationTime         int64
	Hits                   int64
	LastAccessTime         int64
	LastStoredTime         int64
	LastUpdateTime         int64
	Version                int64
	EvictionCriteriaNumber int64
	Ttl                    int64
}

func (ev1 EntryView) Equal(ev2 EntryView) bool {
	if !bytes.Equal(ev1.Key.Buffer, ev2.Key.Buffer) || !bytes.Equal(ev1.Value.Buffer, ev2.Value.Buffer) {
		return false
	}
	if ev1.Cost != ev2.Cost || ev1.CreationTime != ev2.CreationTime || ev1.ExpirationTime != ev2.ExpirationTime || ev1.Hits != ev2.Hits {
		return false
	}
	if ev1.LastAccessTime != ev2.LastAccessTime || ev1.LastStoredTime != ev2.LastStoredTime || ev1.LastUpdateTime != ev2.LastUpdateTime {
		return false
	}
	if ev1.Version != ev2.Version || ev1.EvictionCriteriaNumber != ev2.EvictionCriteriaNumber || ev1.Ttl != ev2.Ttl {
		return false
	}
	return true
}

type Error struct {
	ErrorCode      int32
	ClassName      string
	Message        string
	StackTrace     []StackTraceElement
	CauseErrorCode int32
	CauseClassName string
}

type StackTraceElement struct {
	declaringClass string
	methodName     string
	fileName       string
	lineNumber     int32
}

func (err *Error) Error() string {
	return err.Message
}
