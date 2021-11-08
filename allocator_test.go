package allocator_test

import (
	"github.com/smcheema/allocator"
	"github.com/stretchr/testify/require"
	"testing"
)

// Premise : test replication by requiring replicas to be assigned to unique nodes.
func TestReplication(t *testing.T) {
	const numRanges = 20
	const rf = 3
	const numNodes = 64
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 0), buildEmptyTags(numNodes))
	ranges := buildRanges(numRanges, rf, buildEmptyDemands(numRanges), buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes).Allocate()
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
		require.True(t, isEachReplicaAssignedToDifferentNode(nodeAssignments))
	}
}

// Premise : test infeasible allocation by setting numNodes < rf. This is deemed infeasible since
// we mandate implicitly replicas to live on separate nodes.
func TestReplicationWithInsufficientNodes(t *testing.T) {
	const numRanges = 20
	const rf = 3
	const numNodes = 1
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 0), buildEmptyTags(numNodes))
	ranges := buildRanges(numRanges, rf, buildEmptyDemands(numRanges), buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes).Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

// Premise : Same as above.
func TestReplicationWithInfeasibleRF(t *testing.T) {
	const numRanges = 20
	const rf = 128
	const numNodes = 64
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 0), buildEmptyTags(numNodes))
	ranges := buildRanges(numRanges, rf, buildEmptyDemands(numRanges), buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes).Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

// Premise : build space-aware nodes and ranges. Require all capacity constraints are respected.
func TestCapacity(t *testing.T) {
	const numRanges = 20
	const rf = 1
	const numNodes = 8
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 10_000), buildEmptyTags(numNodes))
	rangeDemands := make([]map[allocator.Resource]int64, numRanges)
	for i := range rangeDemands {
		rangeDemands[i] = map[allocator.Resource]int64{allocator.DiskResource: int64(i)}
	}
	ranges := buildRanges(numRanges, rf, rangeDemands, buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes, allocator.WithResources()).Allocate()
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
	}
}

// Premise : Same as above + replication.
func TestCapacityTogetherWithReplication(t *testing.T) {
	const numRanges = 5
	const rf = 3
	const numNodes = 3
	clusterCapacities := []int64{90, 90, 90}
	rangeSizeDemands := []int64{25, 10, 12, 11, 10}
	rangeDemands := make([]map[allocator.Resource]int64, numRanges)
	for i := range rangeDemands {
		rangeDemands[i] = map[allocator.Resource]int64{allocator.DiskResource: rangeSizeDemands[i]}
	}
	nodes := buildNodes(numNodes, clusterCapacities, buildEmptyTags(numNodes))
	ranges := buildRanges(numRanges, rf, rangeDemands, buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes, allocator.WithResources()).Allocate()
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
		require.True(t, isEachReplicaAssignedToDifferentNode(nodeAssignments))
	}
	require.True(t, nodeCapacityIsRespected(allocation, clusterCapacities, rangeSizeDemands))
}

// Premise : test unhappy path and ensure RF is accounted inside capacity computations.
func TestCapacityWithInfeasibleRF(t *testing.T) {
	const numRanges = 5
	const rf = 5
	const numNodes = 3
	clusterCapacities := []int64{90, 90, 90}
	rangeSizeDemands := []int64{25, 10, 12, 11, 10}
	rangeDemands := make([]map[allocator.Resource]int64, numRanges)
	for i := range rangeDemands {
		rangeDemands[i] = map[allocator.Resource]int64{allocator.DiskResource: rangeSizeDemands[i]}
	}
	nodes := buildNodes(numNodes, clusterCapacities, buildEmptyTags(numNodes))
	ranges := buildRanges(numRanges, rf, rangeDemands, buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes, allocator.WithResources()).Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

// Premise : test unhappy path and ensure we are not allocating when impossible to do so.
func TestCapacityWithInsufficientNodes(t *testing.T) {
	const numRanges = 10
	const rf = 1
	const numNodes = 3
	nodes := buildNodes(numNodes, []int64{70, 70, 70}, buildEmptyTags(numNodes))
	rangeSizeDemands := [numRanges]int64{85, 75, 12, 11, 10, 9, 8, 7, 6, 6}
	rangeDemands := make([]map[allocator.Resource]int64, numRanges)
	for i := range rangeDemands {
		rangeDemands[i] = map[allocator.Resource]int64{allocator.DiskResource: rangeSizeDemands[i]}
	}
	ranges := buildRanges(numRanges, rf, rangeDemands, buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes, allocator.WithResources()).Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

// Premise : check tag affinity works on small cluster and range-set.
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
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 0), nodeTags)
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

// Premise : validate failure upon orthogonal tag sets.
func TestTagsWithNonviableNodes(t *testing.T) {
	const numRanges = 1
	const rf = 1
	const numNodes = 1
	nodeTags := [][]string{{"tag=A"}}
	rangeTags := [][]string{{"tag=B"}}
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 0), nodeTags)
	ranges := buildRanges(numRanges, rf, make([]map[allocator.Resource]int64, numRanges), rangeTags)
	status, allocation := allocator.New(ranges, nodes, allocator.WithTagMatching()).Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

// Premise : allocate once, force allocator to modify prior allocation due to modified tags, ensure impossible to do
// so due to low maxChurn limit.
func TestMaxChurnWithInfeasibleLimit(t *testing.T) {
	const numRanges = 3
	const rf = 3
	const numNodes = 6
	nodeTags := [][]string{
		{"tag=A"},
		{"tag=A"},
		{"tag=A"},
		{"tag=A"},
		{"tag=A"},
		{"tag=A"},
	}
	rangeTags := [][]string{
		{"tag=A"},
		{"tag=A"},
		{"tag=A"},
	}
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 0), nodeTags)
	ranges := buildRanges(numRanges, rf, buildEmptyDemands(numRanges), rangeTags)
	status, allocation := allocator.New(ranges, nodes, allocator.WithTagMatching()).Allocate()
	require.True(t, status)

	const maxChurn = 1
	newTags:=buildEmptyTags(numNodes)
	for index := 1; index < numNodes; index++ {
	nodes.UpdateNodeTags(allocator.NodeID(index), newTags[index])
	}

	//badNodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 0), buildEmptyTags(numNodes))
	status, allocation = allocator.New(ranges, nodes, allocator.WithTagMatching(), allocator.WithChurnMinimized(), allocator.WithMaxChurn(maxChurn), allocator.WithPriorAssignment(allocation)).Allocate()
	require.False(t, status)
	require.Nil(t, allocation)
}

// Premise : define ranges/nodes with respective demands/resources and ensure the load spread
// across resources is within some interval. In this case -> [ideal distribution * 0.8, ideal distribution * 1.2] (20% variance from ideal).
func TestQPSandDiskBalancing(t *testing.T) {
	const numRanges = 12
	const rf = 1
	const numNodes = 6
	nodes := buildNodes(numNodes, nodeCapacitySupplier(numNodes, 10_000), buildEmptyTags(numNodes))
	rangeDemands := make([]map[allocator.Resource]int64, numRanges)
	sizeDemands := 0
	qpsDemands := 0
	for i := range rangeDemands {
		rangeDemands[i] = map[allocator.Resource]int64{allocator.DiskResource: int64(i), allocator.Qps: int64(i)}
		sizeDemands += i
		qpsDemands += i
	}
	ranges := buildRanges(numRanges, rf, rangeDemands, buildEmptyTags(numRanges))
	status, allocation := allocator.New(ranges, nodes, allocator.WithResources()).Allocate()
	require.True(t, status)
	reasonableVariance := 0.2
	idealSizeAllocation := float64(sizeDemands+qpsDemands) / float64(numNodes)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
	}
	nodeConsumption := make(map[allocator.NodeID]int64)
	for rID, nodeAssignments := range allocation {
		for _, nID := range nodeAssignments {
			nodeConsumption[nID] += 2 * int64(rID)
		}
	}
	for _, consumption := range nodeConsumption {
		require.True(t, (float64(consumption) >= (1-reasonableVariance)*idealSizeAllocation) && (float64(consumption) <= (1+reasonableVariance)*idealSizeAllocation))
	}
}

func buildNodes(numNodes int64, nodeCapacities []int64, tags [][]string) allocator.NodeMap {
	//nodes := make([]allocator.Node, numNodes)
	nodes := make(allocator.NodeMap)
	var index int64
	for index = 0; index < numNodes; index++ {
		nodes.AddNode(
			allocator.NodeID(index),
			tags[index],
			map[allocator.Resource]int64{allocator.DiskResource: nodeCapacities[index]},
			)
	}
	return nodes
}

func buildRanges(numRanges int64, rf int, demands []map[allocator.Resource]int64, tags [][]string) allocator.RangeMap {
	rangesToAllocate := make(allocator.RangeMap)
	var index int64
	for index = 0; index < numRanges; index++ {
		rangesToAllocate.AddRange(
			allocator.RangeID(index),
			rf,
			tags[index],
			demands[index])
	}
	return rangesToAllocate
}

func nodeCapacitySupplier(clusterSize int64, capacity int64) []int64 {
	nodeCapacities := make([]int64, clusterSize)
	for index := 0; index < len(nodeCapacities); index++ {
		nodeCapacities[index] = capacity
	}
	return nodeCapacities
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
