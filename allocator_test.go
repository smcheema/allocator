package allocator

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestGivenOnlyReplicationConstraintsTHENAppropriateReplicasAllocated(t *testing.T) {
	rangeSizeLimit := 20
	replicationFactor := 3
	testClusterSize := int64(64)
	testConfig := initConfiguration(buildCluster(testClusterSize), replicationFactor)
	rangesToAllocate := make([]_range, rangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range {
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: nil,
		}
	}
	for rangeIndex := range rangesToAllocate {
		allocator := Allocator(initAllocator(rangesToAllocate[:rangeIndex], testConfig))
		allocator.addAssignLikeReplicasToDifferentNodesConstraint()
		status, allocation := allocator.allocate()
		require.Equal(t, status, true)
		for _, nodeAssignments := range allocation {
			require.Equal(t, len(nodeAssignments), replicationFactor)
			require.True(t, isValidNodeAssignment(nodeAssignments, testClusterSize))
			require.True(t, isEachReplicaAssignedToDifferentNode(nodeAssignments, testClusterSize))
		}
	}
}

func TestGivenInsufficientNodesTHENAllocationFails(t *testing.T) {
	rangeSizeLimit := 20
	replicationFactor := 3
	testClusterSize := int64(1)
	testConfig := initConfiguration(buildCluster(testClusterSize), replicationFactor)
	rangesToAllocate := make([]_range, rangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range {
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: nil,
		}
	}
	allocator := Allocator(initAllocator(rangesToAllocate, testConfig))
	allocator.addAssignLikeReplicasToDifferentNodesConstraint()
	status, allocation := allocator.allocate()
	require.Equal(t, status, false)
	require.Nil(t, allocation)
}

func TestGivenInfeasibleReplicationFactorTHENAllocationFails(t *testing.T) {
	rangeSizeLimit := 20
	replicationFactor := 128
	testClusterSize := int64(64)
	testConfig := initConfiguration(buildCluster(testClusterSize), replicationFactor)
	rangesToAllocate := make([]_range, rangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range {
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: nil,
		}
	}
	allocator := Allocator(initAllocator(rangesToAllocate, testConfig))
	allocator.addAssignLikeReplicasToDifferentNodesConstraint()
	status, allocation := allocator.allocate()
	require.Equal(t, status, false)
	require.Nil(t, allocation)
}

func buildCluster(clusterSize int64) Cluster {
	diskSpaceDemand := "DiskSpaceDemand"
	cluster := make(Cluster, clusterSize)
	clusterCapacities := nodeCapacitySupplier(clusterSize)
	for index := 0; index < len(cluster); index++ {
		cluster[index] = node {
			nodeId: 	 NodeId(index),
			tags: 		 Tags{},
			resources: 	 Resources{Resource(diskSpaceDemand): ResourceAmount(clusterCapacities[index])},
		}
	}
	return cluster
}

func nodeCapacitySupplier(clusterSize int64) []float64 {
	minCapacity := 5000.0
	maxCapacity := 5_000_000.0
	nodeCapacities := make([]float64, clusterSize)
	for index := 0; index < len(nodeCapacities); index++ {
		nodeCapacities[index] = generateRandomFloatInRange(minCapacity, maxCapacity)
	}
	return nodeCapacities
}

func generateRandomFloatInRange(lower float64, upper float64) float64 {
	return lower + rand.Float64() * (upper - lower)
}

func isValidNodeAssignment(nodeIds []NodeId, clusterSize int64) bool {
	for index := 0; index < len(nodeIds); index++ {
		if nodeIds[index] < 0 || int64(nodeIds[index]) > clusterSize {
			return false
		}
	}
	return true
}

func isEachReplicaAssignedToDifferentNode(nodeIds []NodeId, clusterSize int64) bool {
	bitMap := make([]int64, clusterSize)
	for index := 0; index < len(nodeIds); index++ {
		if bitMap[nodeIds[index]] == 1 {
			return false
		} else {
			bitMap[nodeIds[index]]++
		}
	}
	return true
}
