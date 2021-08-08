package allocator

import (
	"github.com/irfansharif/or-tools/cpsatsolver"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLibraryImport(t *testing.T) {
	var _ cpsatsolver.Model
}

func TestAllocationForThreeRanges(t *testing.T) {
	ranges := [3]_range{{tag: 0}, {tag: 1}, {tag: 1 << 1}}
	allocations := allocate(ranges[0:2])
	for replicaCount := 0; replicaCount < len(allocations); replicaCount++ {
		sum := 0
		for nodeCount := 0; nodeCount < len(allocations[replicaCount]); nodeCount++ {
			sum += allocations[replicaCount][nodeCount]
			require.LessOrEqual(t, allocations[replicaCount][nodeCount], 1)
		}
		require.Equal(t, sum, ReplicationFactor)
	}
}
