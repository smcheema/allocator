package allocator_test

import (
	"github.com/irfansharif/allocator"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestReplication(t *testing.T) {
	const numRanges = 20
	const rf = 3
	const numNodes = 64
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 0, 1), buildEmptyTags(numNodes))
	ranges := buildRanges(numRanges, rf, buildEmptyDemands(numRanges), buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes).Allocate()
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
		require.True(t, isEachReplicaAssignedToDifferentNode(nodeAssignments))
	}
}

func TestReplicationWithInsufficientNodes(t *testing.T) {
	const numRanges = 20
	const rf = 3
	const numNodes = 1
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 0, 1), buildEmptyTags(numNodes))
	ranges := buildRanges(numRanges, rf, buildEmptyDemands(numRanges), buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes).Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestReplicationWithInfeasibleRF(t *testing.T) {
	const numRanges = 20
	const rf = 128
	const numNodes = 64
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 0, 1), buildEmptyTags(numNodes))
	ranges := buildRanges(numRanges, rf, buildEmptyDemands(numRanges), buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes).Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestCapacity(t *testing.T) {
	const numRanges = 200
	const rf = 3
	const numNodes = 80
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 8_000, 10_000), buildEmptyTags(numNodes))
	rangeDemands := make([]map[allocator.Resource]int64, numRanges)
	for i := range rangeDemands {
		rangeDemands[i] = map[allocator.Resource]int64{allocator.DiskResource: int64(i)}
	}
	ranges := buildRanges(numRanges, rf, rangeDemands, buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes, allocator.WithNodeCapacity()).Allocate()
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
	}
}

func TestCapacityWithCappedSizes(t *testing.T) {
	const numRanges = 10
	const rf = 1
	const numNodes = 3
	nodes := buildNodes(numNodes, []int64{70, 80, 90}, buildEmptyTags(numNodes))
	rangeSizeDemands := [numRanges]int64{85, 75, 12, 11, 10, 9, 8, 7, 6, 6}
	rangeDemands := make([]map[allocator.Resource]int64, numRanges)
	for i := range rangeDemands {
		rangeDemands[i] = map[allocator.Resource]int64{allocator.DiskResource: rangeSizeDemands[i]}
	}
	ranges := buildRanges(numRanges, rf, rangeDemands, buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes, allocator.WithNodeCapacity()).Allocate()
	expectedAllocation := allocator.Allocation{
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

func TestCapacityTogetherWithReplication(t *testing.T) {
	const numRanges = 5
	const rf = 3
	const numNodes = 3
	clusterCapacities := []int64{70, 80, 90}
	rangeSizeDemands := []int64{25, 10, 12, 11, 10}
	rangeDemands := make([]map[allocator.Resource]int64, numRanges)
	for i := range rangeDemands {
		rangeDemands[i] = map[allocator.Resource]int64{allocator.DiskResource: rangeSizeDemands[i]}
	}
	nodes := buildNodes(numNodes, clusterCapacities, buildEmptyTags(numNodes))
	ranges := buildRanges(numRanges, rf, rangeDemands, buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes, allocator.WithNodeCapacity()).Allocate()
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
		require.True(t, isEachReplicaAssignedToDifferentNode(nodeAssignments))
	}
	require.True(t, nodeCapacityIsRespected(allocation, clusterCapacities, rangeSizeDemands))
}

func TestCapacityWithInfeasibleRF(t *testing.T) {
	const numRanges = 5
	const rf = 5
	const numNodes = 3
	clusterCapacities := []int64{70, 80, 90}
	rangeSizeDemands := []int64{25, 10, 12, 11, 10}
	rangeDemands := make([]map[allocator.Resource]int64, numRanges)
	for i := range rangeDemands {
		rangeDemands[i] = map[allocator.Resource]int64{allocator.DiskResource: rangeSizeDemands[i]}
	}
	nodes := buildNodes(numNodes, clusterCapacities, buildEmptyTags(numNodes))
	ranges := buildRanges(numRanges, rf, rangeDemands, buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes, allocator.WithNodeCapacity()).Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestCapacityWithInsufficientNodes(t *testing.T) {
	const numRanges = 10
	const rf = 1
	const numNodes = 3
	nodes := buildNodes(numNodes, []int64{70, 80, 80}, buildEmptyTags(numNodes))
	rangeSizeDemands := [numRanges]int64{85, 75, 12, 11, 10, 9, 8, 7, 6, 6}
	rangeDemands := make([]map[allocator.Resource]int64, numRanges)
	for i := range rangeDemands {
		rangeDemands[i] = map[allocator.Resource]int64{allocator.DiskResource: rangeSizeDemands[i]}
	}
	ranges := buildRanges(numRanges, rf, rangeDemands, buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes, allocator.WithNodeCapacity()).Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestTagsWithViableNodes(t *testing.T) {
	const numRanges = 3
	const rf = 1
	const numNodes = 3
	nodeTags := [][]string{
		{"a=ant", "b=bus", "b=bin", "d=dog"},
		{"a=all", "b=bus", "e=eat", "f=fun"},
		{"a=art", "b=bin", "e=ear", "f=fur"},
	}
	rangeTags := [][]string{
		{"a=art"},
		{"e=eat"},
		{"a=ant", "b=bus"},
	}
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 0, 1), nodeTags)
	ranges := buildRanges(numRanges, rf, make([]map[allocator.Resource]int64, numRanges), rangeTags)
	expectedAllocation := allocator.Allocation{
		0: {2},
		1: {1},
		2: {0},
	}
	status, allocation := allocator.New(ranges, nodes, allocator.WithTagMatching()).Allocate()
	require.True(t, status)
	require.Equal(t, expectedAllocation, allocation)
}

func TestTagsWithNonviableNodes(t *testing.T) {
	const numRanges = 1
	const rf = 1
	const numNodes = 1
	nodeTags := [][]string{{"tag=A"}}
	rangeTags := [][]string{{"tag=B"}}
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 0, 1), nodeTags)
	ranges := buildRanges(numRanges, rf, make([]map[allocator.Resource]int64, numRanges), rangeTags)
	status, allocation := allocator.New(ranges, nodes, allocator.WithTagMatching()).Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

func TestQPS(t *testing.T) {
	const numRanges = 20
	const rf = 1
	const numNodes = 8
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 8_000, 10_000), buildEmptyTags(numNodes))
	rangeDemands := make([]map[allocator.Resource]int64, numRanges)
	for i := range rangeDemands {
		rangeDemands[i] = map[allocator.Resource]int64{allocator.DiskResource: int64(i), allocator.Qps: int64(i)}
	}
	ranges := buildRanges(numRanges, rf, rangeDemands, buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes, allocator.WithNodeCapacity()).Allocate()
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
	}
}

func buildNodes(numNodes int64, nodeCapacities []int64,tags [][]string) []allocator.Node {
	nodes := make([]allocator.Node, numNodes)
	for index := 0; index < len(nodes); index++ {
		nodes[index] = allocator.NewNode(
			allocator.NodeID(index),
			tags[index],
			map[allocator.Resource]int64{allocator.DiskResource: nodeCapacities[index]})
	}
	return nodes
}

func buildRanges(numRanges int64, rf int, demands []map[allocator.Resource]int64, tags [][]string) []allocator.Range {
	rangesToAllocate := make([]allocator.Range, numRanges)
	for i := range rangesToAllocate {
		rangesToAllocate[i] = allocator.NewRange(
			allocator.RangeID(i),
			rf,
			tags[i],
			demands[i])
	}
	return rangesToAllocate
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

func isValidNodeAssignment(nodeIDs []allocator.NodeID, clusterSize int64) bool {
	for _, nodeID := range nodeIDs {
		if nodeID < 0 || int64(nodeID) > clusterSize {
			return false
		}
	}
	return true
}

func isEachReplicaAssignedToDifferentNode(nodeIDs []allocator.NodeID) bool {
	nodeIdSet := make(map[allocator.NodeID]struct{})
	for _, nodeID := range nodeIDs {
		if _, found := nodeIdSet[nodeID]; found {
			return false
		} else {
			nodeIdSet[nodeID] = struct{}{}
		}
	}
	return true
}

func nodeCapacityIsRespected(allocation map[allocator.RangeID][]allocator.NodeID, nodeCapacities []int64, rangeDemands []int64) bool {
	inUseCapacity := make(map[allocator.NodeID]int64)
	for rangeID, nodeAssignments := range allocation {
		for _, node := range nodeAssignments {
			inUseCapacity[node] += rangeDemands[rangeID]
		}
	}
	for nodeId, nodeCapacityConsumed := range inUseCapacity {
		if nodeCapacityConsumed > nodeCapacities[nodeId] {
			return false
		}
	}
	return true
}

func buildEmptyTags(len int) [][]string {
	ret := make([][]string, len)
	for i := range ret {
		ret[i] = make([]string, 0)
	}
	return ret
}

func buildEmptyDemands(len int) []map[allocator.Resource]int64 {
	ret := make([]map[allocator.Resource]int64, len)
	for i := range ret {
		ret[i] = make(map[allocator.Resource]int64)
	}
	return ret
}
