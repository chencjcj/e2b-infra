package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMeminfoCount(t *testing.T) {
	cases := map[string]struct {
		rest    string
		want    uint64
		wantErr bool
	}{
		"standard":     {"    1000", 1000, false},
		"with kB unit": {" 2048 kB", 2048, false},
		"empty":        {"", 0, true},
		"not a number": {" abc kB", 0, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := parseMeminfoCount(tc.rest)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestExtractAvg10(t *testing.T) {
	pairs := []string{"avg10=1.23", "avg60=4.56", "avg300=7.89", "total=42"}
	got, err := extractAvg10(pairs)
	require.NoError(t, err)
	assert.InDelta(t, 1.23, got, 0.0001)
}

func TestExtractAvg10_Missing(t *testing.T) {
	_, err := extractAvg10([]string{"avg60=0.00"})
	require.Error(t, err)
}

func TestSafeInt64(t *testing.T) {
	assert.Equal(t, int64(0), safeInt64(0))
	assert.Equal(t, int64(1234), safeInt64(1234))
	// overflow case: uint64 max → clamps to int64 max
	assert.Equal(t, int64(^uint64(0)>>1), safeInt64(^uint64(0)))
}
