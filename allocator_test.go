package allocator_test

import (
	"github.com/irfansharif/allocator"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestGIVENReplicationConstraintTHENAppropriateReplicasAllocated(t *testing.T) {
	const RangeSizeLimit = 20
	const ReplicationFactor = 3
	const TestClusterSize = 64
	builtCluster := buildCluster(TestClusterSize, nodeCapacitySupplier(TestClusterSize, 0, 1))
	rangesToAllocate := make([]allocator.Range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = allocator.NewRange(allocator.RangeID(initIndex), ReplicationFactor, nil, nil)
	}
	for rangeIndex := range rangesToAllocate {
		underTest := allocator.New(rangesToAllocate[:rangeIndex], builtCluster)
		status, allocation := underTest.Allocate()
		require.True(t, status)
		for _, nodeAssignments := range allocation {
			require.Equal(t, len(nodeAssignments), ReplicationFactor)
			require.True(t, isValidNodeAssignment(nodeAssignments, TestClusterSize))
			require.True(t, isEachReplicaAssignedToDifferentNode(nodeAssignments, TestClusterSize))
		}
	}
}

func TestGIVENReplicationConstraintWHENInsufficientNodesTHENAllocationFails(t *testing.T) {
	const RangeSizeLimit = 20
	const ReplicationFactor = 3
	const TestClusterSize = 1
	builtCluster := buildCluster(TestClusterSize, nodeCapacitySupplier(TestClusterSize, 0, 1))
	rangesToAllocate := make([]allocator.Range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = allocator.NewRange(allocator.RangeID(initIndex), ReplicationFactor, nil, nil)
	}
	underTest := allocator.New(rangesToAllocate, builtCluster)
	status, allocation := underTest.Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestGIVENReplicationConstraintWHENInfeasibleReplicationFactorTHENAllocationFails(t *testing.T) {
	const RangeSizeLimit = 20
	const ReplicationFactor = 128
	const TestClusterSize = 64
	builtCluster := buildCluster(TestClusterSize, nodeCapacitySupplier(TestClusterSize, 0, 1))
	rangesToAllocate := make([]allocator.Range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = allocator.NewRange(allocator.RangeID(initIndex), ReplicationFactor, nil, nil)
	}
	underTest := allocator.New(rangesToAllocate, builtCluster)
	status, allocation := underTest.Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestGIVENCapacityConstraintWHENMultipleAllocationsPossibleTHENAllocationSucceeds(t *testing.T) {
	const RangeSizeLimit = 20
	const ReplicationFactor = 1
	const TestClusterSize = 8
	builtCluster := buildCluster(TestClusterSize, nodeCapacitySupplier(TestClusterSize, 8_000, 10_000))
	rangesToAllocate := make([]allocator.Range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = allocator.NewRange(
			allocator.RangeID(initIndex),
			ReplicationFactor,
			nil,
			map[allocator.Resource]int64{allocator.DiskResource: int64(initIndex)})
	}
	underTest := allocator.New(rangesToAllocate, builtCluster)
	status, allocation := underTest.Allocate(allocator.WithNodeCapacityConstraint())
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), ReplicationFactor)
		require.True(t, isValidNodeAssignment(nodeAssignments, TestClusterSize))
	}
}

func TestGIVENCapacityConstraintWHENSingleAllocationPossibleTHENAllocationSucceeds(t *testing.T) {
	const RangeSizeLimit = 10
	const ReplicationFactor = 1
	const TestClusterSize = 3
	builtCluster := buildCluster(TestClusterSize, []int64{70, 80, 90})
	rangesToAllocate := make([]allocator.Range, RangeSizeLimit)
	rangeDiskSpaceDemands := [RangeSizeLimit]int64{85, 75, 12, 11, 10, 9, 8, 7, 6, 6}
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = allocator.NewRange(
			allocator.RangeID(initIndex),
			ReplicationFactor,
			nil,
			map[allocator.Resource]int64{allocator.DiskResource: rangeDiskSpaceDemands[initIndex]})
	}
	underTest := allocator.New(rangesToAllocate, builtCluster)
	status, allocation := underTest.Allocate(allocator.WithNodeCapacityConstraint())
	expectedAllocation := map[allocator.RangeID][]allocator.NodeID{
		0: {2},
		1: {1},
		2: {0},
		3: {0},
		4: {0},
		5: {0},
		6: {0},
		7: {0},
		8: {0},
		9: {0},
	}
	require.True(t, status)
	require.Equal(t, expectedAllocation, allocation)
}

func TestGIVENCapacityConstraintWithReplicationWHENMultipleAllocationsPossibleTHENAllocationSucceeds(t *testing.T) {
	const RangeSizeLimit = 5
	const ReplicationFactor = 3
	const TestClusterSize = 3
	clusterCapacities := []int64{70, 80, 90}
	rangeDiskSpaceDemands := []int64{25, 10, 12, 11, 10}
	builtCluster := buildCluster(TestClusterSize, clusterCapacities)
	rangesToAllocate := make([]allocator.Range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = allocator.NewRange(
			allocator.RangeID(initIndex),
			ReplicationFactor,
			nil,
			map[allocator.Resource]int64{allocator.DiskResource: rangeDiskSpaceDemands[initIndex]})
	}
	underTest := allocator.New(rangesToAllocate, builtCluster)
	status, allocation := underTest.Allocate(allocator.WithNodeCapacityConstraint())
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), ReplicationFactor)
		require.True(t, isValidNodeAssignment(nodeAssignments, TestClusterSize))
		require.True(t, isEachReplicaAssignedToDifferentNode(nodeAssignments, TestClusterSize))
	}
	require.True(t, nodeCapacityIsRespected(allocation, clusterCapacities, rangeDiskSpaceDemands))
}

func TestGIVENCapacityConstraintWithReplicationWHENInsufficientResourcesTHENAllocationFails(t *testing.T) {
	const RangeSizeLimit = 5
	const ReplicationFactor = 5
	const TestClusterSize = 3
	clusterCapacities := []int64{70, 80, 90}
	rangeDiskSpaceDemands := []int64{25, 10, 12, 11, 10}
	builtCluster := buildCluster(TestClusterSize, clusterCapacities)
	rangesToAllocate := make([]allocator.Range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = allocator.NewRange(
			allocator.RangeID(initIndex),
			ReplicationFactor,
			nil,
			map[allocator.Resource]int64{allocator.DiskResource: rangeDiskSpaceDemands[initIndex]})
	}
	underTest := allocator.New(rangesToAllocate, builtCluster)
	status, allocation := underTest.Allocate(allocator.WithNodeCapacityConstraint())
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestGIVENCapacityConstraintWHENInsufficientNodeCapacityTHENAllocationFails(t *testing.T) {
	const RangeSizeLimit = 10
	const ReplicationFactor = 1
	const TestClusterSize = 3
	builtCluster := buildCluster(TestClusterSize, []int64{70, 80, 80})
	rangesToAllocate := make([]allocator.Range, RangeSizeLimit)
	rangeDiskSpaceDemands := [RangeSizeLimit]int64{85, 75, 12, 11, 10, 9, 8, 7, 6, 6}
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = allocator.NewRange(
			allocator.RangeID(initIndex),
			ReplicationFactor,
			nil,
			map[allocator.Resource]int64{allocator.DiskResource: rangeDiskSpaceDemands[initIndex]})
	}
	underTest := allocator.New(rangesToAllocate, builtCluster)
	status, allocation := underTest.Allocate(allocator.WithNodeCapacityConstraint())
	require.False(t, status)
	require.Nil(t, allocation)
}

func buildCluster(clusterSize int64, nodeCapacities []int64) []allocator.Node {
	cluster := make([]allocator.Node, clusterSize)
	for index := 0; index < len(cluster); index++ {
		cluster[index] = allocator.NewNode(
			allocator.NodeID(index),
			nil,
			map[allocator.Resource]int64{allocator.DiskResource: nodeCapacities[index]})
	}
	return cluster
}

func nodeCapacitySupplier(clusterSize int64, minCapacity int64, maxCapacity int64) []int64 {
	nodeCapacities := make([]int64, clusterSize)
	for index := 0; index < len(nodeCapacities); index++ {
		nodeCapacities[index] = generateRandomIntInRange(minCapacity, maxCapacity)
	}
	return nodeCapacities
}

func generateRandomIntInRange(lower int64, upper int64) int64 {
	return rand.Int63n(upper-lower) + lower
}

func isValidNodeAssignment(nodeIds []allocator.NodeID, clusterSize int64) bool {
	for index := 0; index < len(nodeIds); index++ {
		if nodeIds[index] < 0 || int64(nodeIds[index]) > clusterSize {
			return false
		}
	}
	return true
}

func isEachReplicaAssignedToDifferentNode(nodeIds []allocator.NodeID, clusterSize int64) bool {
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

func nodeCapacityIsRespected(allocation map[allocator.RangeID][]allocator.NodeID, nodeCapacities []int64, rangeDemands []int64) bool {
	verifierMap := make(map[allocator.NodeID]int64)
	for _range, nodeAssignments := range allocation {
		for _, node := range nodeAssignments {
			verifierMap[node] += rangeDemands[_range]
		}
	}
	for nodeId, nodeCapacityConsumed := range verifierMap {
		if nodeCapacityConsumed > nodeCapacities[nodeId] {
			return false
		}
	}
	return true
}
