package pred

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

const (
	sqlID = iota
	andID
	betweenID
	equalID
	greaterlessID
	likeID
	ilikeID
	inID
	instanceOfID
	notEqualID
	notID
	orID
	regexID
	falseID
	trueID
	// pagingID
	// partitionID
	// nilObjectID
)

const factoryID = -20

type Predicate interface {
	serialization.IdentifiedDataSerializable
	fmt.Stringer
	// disallow creating predicates by the user
	enforcePredicate()
}

type PagingPredicate interface {
	Predicate
	Reset()
	NextPage()
	PreviousPage()
	Page() int
	SetPage(page int)
	PageSize() int
}
