package cp

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestObjectNameForProxy(t *testing.T) {
	n, err := objectNameForProxy("test@default")
	require.NoError(t, err)
	require.Equal(t, "test", n)
	n, err = objectNameForProxy("test@custom")
	require.NoError(t, err)
	require.Equal(t, "test", n)
}

func TestObjectNameForProxyWithEmptyObjectName(t *testing.T) {
	_, err := objectNameForProxy("@default")
	assert.Error(t, err, "Object name cannot be empty string")
	_, err = objectNameForProxy("     @default")
	assert.Error(t, err, "Object name cannot be empty string")
}

func TestObjectNameForProxyWithEmptyProxyName(t *testing.T) {
	_, err := objectNameForProxy("test@")
	assert.Error(t, err, "Custom CP group name cannot be empty string")
	_, err = objectNameForProxy("test@    ")
	assert.Error(t, err, "Custom CP group name cannot be empty string")
}

func TestWithoutDefaultGroupName(t *testing.T) {
	n, err := withoutDefaultGroupName("test@default")
	require.NoError(t, err)
	require.Equal(t, n, "test")

	n, err = withoutDefaultGroupName("test@DEFAULT")
	require.NoError(t, err)
	require.Equal(t, n, "test")

	n, err = withoutDefaultGroupName("test@custom")
	require.NoError(t, err)
	require.Equal(t, n, "test@custom")
}

func TestWithoutDefaultGroupName_WithMultipleGroupNames(t *testing.T) {
	_, err := withoutDefaultGroupName("test@default@@default")
	require.Error(t, err, "Custom group name must be specified at most once")
}

func TestWithoutDefaultGroupName_WithMetadataGroupName(t *testing.T) {
	_, err := withoutDefaultGroupName("test@METADATA")
	require.Error(t, err, "CP data structures cannot run on the METADATA CP group!")

	_, err = withoutDefaultGroupName("test@metadata")
	require.Error(t, err, "CP data structures cannot run on the METADATA CP group!")
}
