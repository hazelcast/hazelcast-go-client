package predicate

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/serialization"
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
