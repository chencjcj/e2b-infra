package procstats

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadHugetlbStats_NonExistentPid(t *testing.T) {
	got, err := ReadHugetlbStats(math.MaxInt32)
	require.NoError(t, err)
	assert.Equal(t, HugetlbStats{}, got)
}

func TestReadHugetlbBytes_NonExistentPid(t *testing.T) {
	got, err := ReadHugetlbBytes(math.MaxInt32)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), got)
}

func TestParseKBLine(t *testing.T) {
	cases := map[string]struct {
		line    string
		want    uint64
		wantErr bool
	}{
		"zero":              {"Private_Hugetlb:       0 kB", 0, false},
		"positive":          {"Shared_Hugetlb:  2048 kB", 2048, false},
		"extra whitespace":  {"Private_Hugetlb:\t\t 4096 kB", 4096, false},
		"missing colon":     {"Private_Hugetlb 0 kB", 0, true},
		"missing value":     {"Private_Hugetlb: kB", 0, true},
		"non-numeric value": {"Private_Hugetlb: abc kB", 0, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := parseKBLine(tc.line)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseHugetlbRollup_TypicalFCProcess(t *testing.T) {
	body := `55555000-55556000 ---p 00000000 00:00 0                                  [rollup]
Rss:                4096 kB
Pss:                4096 kB
Shared_Clean:          0 kB
Shared_Dirty:          0 kB
Private_Clean:      4096 kB
Private_Dirty:         0 kB
Referenced:         4096 kB
Anonymous:          4096 kB
LazyFree:              0 kB
AnonHugePages:         0 kB
ShmemPmdMapped:        0 kB
FilePmdMapped:         0 kB
Shared_Hugetlb:     2048 kB
Private_Hugetlb:   4096 kB
Swap:                  0 kB
SwapPss:               0 kB
Locked:                0 kB
`
	got, err := parseHugetlbRollup(strings.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, HugetlbStats{
		PrivateBytes: 4096 * 1024,
		SharedBytes:  2048 * 1024,
	}, got)
	assert.Equal(t, uint64((2048+4096)*1024), got.Total())
}

func TestParseHugetlbRollup_NoHugetlbLines(t *testing.T) {
	body := `Rss: 4096 kB
Pss: 4096 kB
`
	got, err := parseHugetlbRollup(strings.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, HugetlbStats{}, got)
}

func TestParseHugetlbRollup_EmptyInput(t *testing.T) {
	got, err := parseHugetlbRollup(strings.NewReader(""))
	require.NoError(t, err)
	assert.Equal(t, HugetlbStats{}, got)
}

func TestParseHugetlbRollup_OnlyPrivate(t *testing.T) {
	body := "Private_Hugetlb:   8192 kB\n"
	got, err := parseHugetlbRollup(strings.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, HugetlbStats{PrivateBytes: 8192 * 1024}, got)
}

func TestParseHugetlbRollup_OnlyShared(t *testing.T) {
	body := "Shared_Hugetlb:   1024 kB\n"
	got, err := parseHugetlbRollup(strings.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, HugetlbStats{SharedBytes: 1024 * 1024}, got)
}

func TestParseHugetlbRollup_MalformedLine(t *testing.T) {
	body := "Private_Hugetlb: nope kB\n"
	_, err := parseHugetlbRollup(strings.NewReader(body))
	require.Error(t, err)
}
