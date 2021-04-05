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

type writeDataFun func(output serialization.DataOutput) error

type predicate struct {
	id           int32
	writeDataFun writeDataFun
}

/*
func newPredicate(id int32, writeDataFun writeDataFun) *predicate {
	return &predicate{
		id:           id,
		writeDataFun: writeDataFun,
	}
}

type predicateFun func() *predicate

func eq(fieldName string, value interface{}) predicateFun {
	return func() *predicate {
		return newPredicate(equalID, func(output serialization.DataOutput) error {
			output.WriteString(fieldName)
			return output.WriteObject(value)
		})
	}
}

func and(predicates ...predicateFun) predicateFun {
	return func() *predicate {
		return newPredicate(andID, func(output serialization.DataOutput) error {
			output.WriteInt32(int32(len(predicates)))
			for _, pred := range predicates {
				if err := output.WriteObject(pred()); err != nil {
					return err
				}
			}
			return nil
		})
	}
}
*/

//type predDecodeHandler func(input serialization.DataInput) (Predicate, error)
//
//var idToPred = map[int32]predDecodeHandler{
//	andID: func(input serialization.DataInput) (Predicate, error) {
//		p := predAnd{}
//		if err := p.ReadData(input); err != nil {
//			return
//		}
//	},
//}
