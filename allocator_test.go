package allocator

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestGIVENReplicationConstraintTHENAppropriateReplicasAllocated(t *testing.T) {
	const RangeSizeLimit = 20
	const ReplicationFactor = 3
	const TestClusterSize = int64(64)
	testConfig := initConfiguration(buildCluster(TestClusterSize, nodeCapacitySupplier(TestClusterSize, 0, 1)), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: nil,
		}
	}
	for rangeIndex := range rangesToAllocate {
		allocator := Allocator(initAllocator(rangesToAllocate[:rangeIndex], testConfig))
		allocator.addAssignLikeReplicasToDifferentNodesConstraint()
		status, allocation := allocator.allocate()
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
	const TestClusterSize = int64(1)
	testConfig := initConfiguration(buildCluster(TestClusterSize, nodeCapacitySupplier(TestClusterSize, 0, 1)), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: nil,
		}
	}
	allocator := Allocator(initAllocator(rangesToAllocate, testConfig))
	allocator.addAssignLikeReplicasToDifferentNodesConstraint()
	status, allocation := allocator.allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestGIVENReplicationConstraintWHENInfeasibleReplicationFactorTHENAllocationFails(t *testing.T) {
	const RangeSizeLimit = 20
	const ReplicationFactor = 128
	const TestClusterSize = int64(64)
	testConfig := initConfiguration(buildCluster(TestClusterSize, nodeCapacitySupplier(TestClusterSize, 0, 1)), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: nil,
		}
	}
	allocator := Allocator(initAllocator(rangesToAllocate, testConfig))
	allocator.addAssignLikeReplicasToDifferentNodesConstraint()
	status, allocation := allocator.allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestGIVENCapacityConstraintWHENMultipleAllocationsPossibleTHENAllocationSucceeds(t *testing.T) {
	const RangeSizeLimit = 20
	const ReplicationFactor = 1
	const TestClusterSize = int64(8)
	testConfig := initConfiguration(buildCluster(TestClusterSize, nodeCapacitySupplier(TestClusterSize, 8_000, 10_000)), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: Demands{SizeOnDiskDemand: ResourceAmount(initIndex)},
		}
	}
	allocator := Allocator(initAllocator(rangesToAllocate, testConfig))
	allocator.addAssignLikeReplicasToDifferentNodesConstraint()
	allocator.addAdhereToNodeDiskSpaceConstraint()
	status, allocation := allocator.allocate()
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), ReplicationFactor)
		require.True(t, isValidNodeAssignment(nodeAssignments, TestClusterSize))
	}
}

func TestGIVENCapacityConstraintWHENSingleAllocationPossibleTHENAllocationSucceeds(t *testing.T) {
	const RangeSizeLimit = 10
	const ReplicationFactor = 1
	const TestClusterSize = int64(3)
	testConfig := initConfiguration(buildCluster(TestClusterSize, []int64{70, 80, 90}), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	rangeDiskSpaceDemands := [RangeSizeLimit]ResourceAmount{85, 75, 12, 11, 10, 9, 8, 7, 6, 6}
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: Demands{SizeOnDiskDemand: rangeDiskSpaceDemands[initIndex]},
		}
	}
	allocator := Allocator(initAllocator(rangesToAllocate, testConfig))
	allocator.addAssignLikeReplicasToDifferentNodesConstraint()
	allocator.addAdhereToNodeDiskSpaceConstraint()
	status, allocation := allocator.allocate()
	expectedAllocation := Assignments{
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

func TestGIVENCapacityConstraintWHENInsufficientNodeCapacityTHENAllocationFails(t *testing.T) {
	const RangeSizeLimit = 10
	const ReplicationFactor = 1
	const TestClusterSize = int64(3)
	testConfig := initConfiguration(buildCluster(TestClusterSize, []int64{70, 80, 80}), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	rangeDiskSpaceDemands := [RangeSizeLimit]ResourceAmount{85, 75, 12, 11, 10, 9, 8, 7, 6, 6}
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: Demands{SizeOnDiskDemand: rangeDiskSpaceDemands[initIndex]},
		}
	}
	allocator := Allocator(initAllocator(rangesToAllocate, testConfig))
	allocator.addAssignLikeReplicasToDifferentNodesConstraint()
	allocator.addAdhereToNodeDiskSpaceConstraint()
	status, allocation := allocator.allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func buildCluster(clusterSize int64, nodeCapacities []int64) Cluster {
	cluster := make(Cluster, clusterSize)
	for index := 0; index < len(cluster); index++ {
		cluster[index] = node{
			nodeId:    NodeId(index),
			tags:      Tags{},
			resources: Resources{DiskCapacityResource: ResourceAmount(nodeCapacities[index])},
		}
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
