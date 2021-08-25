package allocator

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestGIVENReplicationConstraintTHENAppropriateReplicasAllocated(t *testing.T) {
	const RangeSizeLimit = 20
	const ReplicationFactor = 3
	const TestClusterSize = ResourceAmount(64)
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
		allocator := initAllocator(rangesToAllocate[:rangeIndex], testConfig)
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
	const TestClusterSize = ResourceAmount(1)
	testConfig := initConfiguration(buildCluster(TestClusterSize, nodeCapacitySupplier(TestClusterSize, 0, 1)), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: nil,
		}
	}
	allocator := initAllocator(rangesToAllocate, testConfig)
	allocator.addAssignLikeReplicasToDifferentNodesConstraint()
	status, allocation := allocator.allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestGIVENReplicationConstraintWHENInfeasibleReplicationFactorTHENAllocationFails(t *testing.T) {
	const RangeSizeLimit = 20
	const ReplicationFactor = 128
	const TestClusterSize = ResourceAmount(64)
	testConfig := initConfiguration(buildCluster(TestClusterSize, nodeCapacitySupplier(TestClusterSize, 0, 1)), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: nil,
		}
	}
	allocator := initAllocator(rangesToAllocate, testConfig)
	allocator.addAssignLikeReplicasToDifferentNodesConstraint()
	status, allocation := allocator.allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestGIVENCapacityConstraintWHENMultipleAllocationsPossibleTHENAllocationSucceeds(t *testing.T) {
	const RangeSizeLimit = 20
	const ReplicationFactor = 1
	const TestClusterSize = ResourceAmount(8)
	testConfig := initConfiguration(buildCluster(TestClusterSize, nodeCapacitySupplier(TestClusterSize, 8_000, 10_000)), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: Demands{SizeOnDiskDemand: ResourceAmount(initIndex)},
		}
	}
	allocator := initAllocator(rangesToAllocate, testConfig)
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
	const TestClusterSize = ResourceAmount(3)
	testConfig := initConfiguration(buildCluster(TestClusterSize, []ResourceAmount{70, 80, 90}), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	rangeDiskSpaceDemands := [RangeSizeLimit]ResourceAmount{85, 75, 12, 11, 10, 9, 8, 7, 6, 6}
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: Demands{SizeOnDiskDemand: rangeDiskSpaceDemands[initIndex]},
		}
	}
	allocator := initAllocator(rangesToAllocate, testConfig)
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

func TestGIVENCapacityConstraintAndReplicationConstraintWHENMultipleAllocationsPossibleTHENAllocationSucceeds(t *testing.T) {
	const RangeSizeLimit = 5
	const ReplicationFactor = 3
	const TestClusterSize = ResourceAmount(3)
	clusterCapacities := []ResourceAmount{70, 80, 90}
	rangeDiskSpaceDemands := []ResourceAmount{25, 10, 12, 11, 10}
	testConfig := initConfiguration(buildCluster(TestClusterSize, clusterCapacities), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: Demands{SizeOnDiskDemand: rangeDiskSpaceDemands[initIndex]},
		}
	}
	allocator := initAllocator(rangesToAllocate, testConfig)
	allocator.addAssignLikeReplicasToDifferentNodesConstraint()
	allocator.addAdhereToNodeDiskSpaceConstraint()
	status, allocation := allocator.allocate()
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), ReplicationFactor)
		require.True(t, isValidNodeAssignment(nodeAssignments, TestClusterSize))
		require.True(t, isEachReplicaAssignedToDifferentNode(nodeAssignments, TestClusterSize))
	}
	require.True(t, nodeCapacityIsRespected(allocation, clusterCapacities, rangeDiskSpaceDemands))
}

func TestGIVENCapacityConstraintAndReplicationConstraintWHENInsufficientResourcesTHENAllocationFails(t *testing.T) {
	const RangeSizeLimit = 5
	const ReplicationFactor = 5
	const TestClusterSize = ResourceAmount(3)
	clusterCapacities := []ResourceAmount{70, 80, 90}
	rangeDiskSpaceDemands := []ResourceAmount{25, 10, 12, 11, 10}
	testConfig := initConfiguration(buildCluster(TestClusterSize, clusterCapacities), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: Demands{SizeOnDiskDemand: rangeDiskSpaceDemands[initIndex]},
		}
	}
	allocator := initAllocator(rangesToAllocate, testConfig)
	allocator.addAssignLikeReplicasToDifferentNodesConstraint()
	allocator.addAdhereToNodeDiskSpaceConstraint()
	status, allocation := allocator.allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestGIVENCapacityConstraintWHENInsufficientNodeCapacityTHENAllocationFails(t *testing.T) {
	const RangeSizeLimit = 10
	const ReplicationFactor = 1
	const TestClusterSize = ResourceAmount(3)
	testConfig := initConfiguration(buildCluster(TestClusterSize, []ResourceAmount{70, 80, 80}), ReplicationFactor)
	rangesToAllocate := make([]_range, RangeSizeLimit)
	rangeDiskSpaceDemands := [RangeSizeLimit]ResourceAmount{85, 75, 12, 11, 10, 9, 8, 7, 6, 6}
	for initIndex := range rangesToAllocate {
		rangesToAllocate[initIndex] = _range{
			rangeId: RangeId(initIndex),
			tags:    nil,
			demands: Demands{SizeOnDiskDemand: rangeDiskSpaceDemands[initIndex]},
		}
	}
	allocator := initAllocator(rangesToAllocate, testConfig)
	allocator.addAssignLikeReplicasToDifferentNodesConstraint()
	allocator.addAdhereToNodeDiskSpaceConstraint()
	status, allocation := allocator.allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func buildCluster(clusterSize ResourceAmount, nodeCapacities []ResourceAmount) Cluster {
	cluster := make(Cluster, clusterSize)
	for index := 0; index < len(cluster); index++ {
		cluster[index] = node{
			nodeId:    NodeId(index),
			tags:      Tags{},
			resources: Resources{DiskCapacityResource: nodeCapacities[index]},
		}
	}
	return cluster
}

func nodeCapacitySupplier(clusterSize ResourceAmount, minCapacity ResourceAmount, maxCapacity ResourceAmount) []ResourceAmount {
	nodeCapacities := make([]ResourceAmount, clusterSize)
	for index := 0; index < len(nodeCapacities); index++ {
		nodeCapacities[index] = generateRandomIntInRange(minCapacity, maxCapacity)
	}
	return nodeCapacities
}

func generateRandomIntInRange(lower ResourceAmount, upper ResourceAmount) ResourceAmount {
	return ResourceAmount(rand.Int63n(int64(upper-lower)) + int64(lower))
}

func isValidNodeAssignment(nodeIds []NodeId, clusterSize ResourceAmount) bool {
	for index := 0; index < len(nodeIds); index++ {
		if nodeIds[index] < 0 || ResourceAmount(nodeIds[index]) > clusterSize {
			return false
		}
	}
	return true
}

func isEachReplicaAssignedToDifferentNode(nodeIds []NodeId, clusterSize ResourceAmount) bool {
	bitMap := make([]ResourceAmount, clusterSize)
	for index := 0; index < len(nodeIds); index++ {
		if bitMap[nodeIds[index]] == 1 {
			return false
		} else {
			bitMap[nodeIds[index]]++
		}
	}
	return true
}

func nodeCapacityIsRespected(allocation Assignments, nodeCapacities []ResourceAmount, rangeDemands []ResourceAmount) bool {
	verifierMap := make(map[NodeId]ResourceAmount)
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
