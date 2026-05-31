package pagepool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testPageSize  = 2 * 1024 * 1024
	testTotalSize = 4 * 1024 * 1024
)

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()
	pool, err := NewPagePool("test-idempotent", testTotalSize, testPageSize, false)
	require.NoError(t, err)

	require.NoError(t, pool.Close())
	require.NoError(t, pool.Close())
	require.NoError(t, pool.Close())
}
