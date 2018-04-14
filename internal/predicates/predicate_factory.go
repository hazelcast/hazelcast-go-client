// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package predicates

import (
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type PredicateFactory struct {
}

func NewPredicateFactory() *PredicateFactory {
	return &PredicateFactory{}
}

func (pf *PredicateFactory) Create(id int32) serialization.IdentifiedDataSerializable {
	switch id {
	case SqlPredicateId:
		return &SqlPredicate{}
	case AndPredicateId:
		return &AndPredicate{}
	case BetweenPredicateId:
		return &BetweenPredicate{}
	case EqualPredicateId:
		return &EqualPredicate{}
	case GreaterlessPredicateId:
		return &GreaterLessPredicate{}
	case LikePredicateId:
		return &LikePredicate{}
	case ILikePredicateId:
		return &ILikePredicate{}
	case InPredicateId:
		return &InPredicate{}
	case InstanceOfPredicateId:
		return &InstanceOfPredicate{}
	case NotEqualPredicateId:
		return &NotEqualPredicate{}
	case NotPredicateId:
		return &NotPredicate{}
	case OrPredicateId:
		return &OrPredicate{}
	case RegexPredicateId:
		return &RegexPredicate{}
	case FalsePredicateId:
		return &FalsePredicate{}
	case TruePredicateId:
		return &TruePredicate{}
	default:
		return nil
	}
}
