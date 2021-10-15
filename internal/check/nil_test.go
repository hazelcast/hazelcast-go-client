package check_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/check"
)

func TestIsNil(t *testing.T) {
	assert.True(t, check.Nil(nil))

}

func TestIsNil_forValueNil_TypeKnown(t *testing.T) {
	var pnt *int
	var i interface{} = pnt
	assert.True(t, check.Nil(i))
}
