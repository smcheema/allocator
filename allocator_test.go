package allocator

import (
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/irfansharif/or-tools/cpsatsolver"
)

func TestLibraryImport(t *testing.T) {
	var _ cpsatsolver.Model
}

func TestAllocationForThreeRanges(t *testing.T) {
	ranges := [3]_range{{tag: 0}, {tag: 1}, {tag: 1 << 1}}
	allocations := allocate(ranges[0:2])
	// assert that row holds a sum of exactly one,
	// the allocator returns an a two dimensional array of assignments,
	// [i][j] --> replica i assigned to node j, hence assert for each row
	// that there exists one and only one value equal to unity.
	for replicaCount := 0; replicaCount < len(allocations); replicaCount++ {
		sum := 0
		for nodeCount := 0; nodeCount < len(allocations[replicaCount]); nodeCount++ {
			sum += allocations[replicaCount][nodeCount]
		}
		require.Equal(t, 1, sum)
	}
}
