package hazelcast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsNil(t *testing.T) {
	assert.True(t, isNil(nil))

}

func TestIsNil_forValueNil_TypeKnown(t *testing.T) {
	var pnt *int
	var i interface{} = pnt
	assert.True(t, isNil(i))
}
