package allocator_test

import (
	"github.com/smcheema/allocator"
	"github.com/stretchr/testify/require"
	"testing"
)

// Premise : test replication by requiring replicas to be assigned to unique nodes.
func TestReplication(t *testing.T) {
	const numReplicas = 20
	const rf = 3
	const numNodes = 64

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i))
	}
	for i := 0; i < numReplicas; i++ {
		clusterState.AddReplica(int64(i), rf)
	}

	status, allocation := allocator.Solve(clusterState)
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
	const numReplicas = 20
	const rf = 3
	const numNodes = 1

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i))
	}
	for i := 0; i < numReplicas; i++ {
		clusterState.AddReplica(int64(i), rf)
	}

	status, allocation := allocator.Solve(clusterState)
	require.False(t, status)
	require.Nil(t, allocation)
}

// Premise : Same as above.
func TestReplicationWithInfeasibleRF(t *testing.T) {
	const numReplicas = 20
	const rf = 128
	const numNodes = 64

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i))
	}
	for i := 0; i < numReplicas; i++ {
		clusterState.AddReplica(int64(i), rf)
	}

	status, allocation := allocator.Solve(clusterState)
	require.False(t, status)
	require.Nil(t, allocation)
}

// Premise : build space-aware nodes and replicas. Require all capacity constraints are respected.
func TestCapacity(t *testing.T) {
	const numReplicas = 20
	const rf = 1
	const numNodes = 8
	const nodeCapacity = 10_000

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithResourceOfNode(allocator.DiskResource, nodeCapacity),
		)
	}
	for i := 0; i < numReplicas; i++ {
		clusterState.AddReplica(
			int64(i),
			rf,
			allocator.WithDemandOfReplica(allocator.DiskResource, int64(i)),
		)
	}

	status, allocation := allocator.Solve(clusterState, allocator.WithResources())
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
	}
}

// Premise : Same as above + replication.
func TestCapacityTogetherWithReplication(t *testing.T) {
	const numReplicas = 5
	const rf = 3
	const numNodes = 3
	clusterCapacities := []int64{90, 90, 90}
	replicaSizeDemands := []int64{25, 10, 12, 11, 10}

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithResourceOfNode(allocator.DiskResource, clusterCapacities[i]),
		)
	}
	for i := 0; i < numReplicas; i++ {
		clusterState.AddReplica(
			int64(i),
			rf,
			allocator.WithDemandOfReplica(allocator.DiskResource, replicaSizeDemands[i]),
		)
	}

	status, allocation := allocator.Solve(clusterState, allocator.WithResources())
	require.True(t, status)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
		require.True(t, isEachReplicaAssignedToDifferentNode(nodeAssignments))
	}
	require.True(t, nodeCapacityIsRespected(allocation, clusterCapacities, replicaSizeDemands))
}

// Premise : test unhappy path and ensure RF is accounted inside capacity computations.
func TestCapacityWithInfeasibleRF(t *testing.T) {
	const numReplicas = 5
	const rf = 5
	const numNodes = 3
	clusterCapacities := []int64{90, 90, 90}
	replicaSizeDemands := []int64{25, 10, 12, 11, 10}

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithResourceOfNode(allocator.DiskResource, clusterCapacities[i]),
		)
	}
	for i := 0; i < numReplicas; i++ {
		clusterState.AddReplica(
			int64(i),
			rf,
			allocator.WithDemandOfReplica(allocator.DiskResource, replicaSizeDemands[i]),
		)
	}

	status, allocation := allocator.Solve(clusterState, allocator.WithResources())
	require.False(t, status)
	require.Nil(t, allocation)
}

// Premise : test unhappy path and ensure we are not allocating when impossible to do so.
func TestCapacityWithInsufficientNodes(t *testing.T) {
	const numReplicas = 10
	const rf = 1
	const numNodes = 3
	clusterCapacities := []int64{70, 70, 70}
	replicaSizeDemands := [numReplicas]int64{85, 75, 12, 11, 10, 9, 8, 7, 6, 6}

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithResourceOfNode(allocator.DiskResource, clusterCapacities[i]),
		)
	}
	for i := 0; i < numReplicas; i++ {
		clusterState.AddReplica(
			int64(i),
			rf,
			allocator.WithDemandOfReplica(allocator.DiskResource, replicaSizeDemands[i]),
		)
	}

	status, allocation := allocator.Solve(clusterState, allocator.WithResources())
	require.False(t, status)
	require.Nil(t, allocation)
}

// Premise : check tag affinity works on small cluster and replica-set.
func TestTagsWithViableNodes(t *testing.T) {
	const numReplicas = 3
	const rf = 1
	const numNodes = 3
	nodeTags := [][]string{
		{"a=ant", "b=bus", "b=bin", "d=dog"},
		{"a=all", "b=bus", "e=eat", "f=fun"},
		{"a=art", "b=bin", "e=ear", "f=fur"},
	}
	replicaTags := [][]string{
		{"a=art"},
		{"e=eat"},
		{"a=ant", "b=bus"},
	}

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithTagsOfNode(nodeTags[i]...),
		)
	}
	for i := 0; i < numReplicas; i++ {
		clusterState.AddReplica(
			int64(i),
			rf,
			allocator.WithTagsOfReplica(replicaTags[i]...),
		)
	}

	expectedAllocation := allocator.Allocation{
		0: {2},
		1: {1},
		2: {0},
	}
	status, allocation := allocator.Solve(clusterState, allocator.WithTagMatching())
	require.True(t, status)
	require.Equal(t, expectedAllocation, allocation)
}

// Premise : validate failure upon orthogonal tag sets.
func TestTagsWithNonviableNodes(t *testing.T) {
	const numReplicas = 1
	const rf = 1
	const numNodes = 1
	nodeTags := [][]string{{"tag=A"}}
	replicaTags := [][]string{{"tag=B"}}

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithTagsOfNode(nodeTags[i]...),
		)
	}
	for i := 0; i < numReplicas; i++ {
		clusterState.AddReplica(
			int64(i),
			rf,
			allocator.WithTagsOfReplica(replicaTags[i]...),
		)
	}

	status, allocation := allocator.Solve(clusterState, allocator.WithTagMatching())
	require.False(t, status)
	require.Nil(t, allocation)
}

// Premise : allocate once, force allocator to modify prior allocation due to modified tags, ensure impossible to do
// so due to low maxChurn limit.
func TestMaxChurnWithInfeasibleLimit(t *testing.T) {
	const numReplicas = 3
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
	replicaTags := [][]string{
		{"tag=A"},
		{"tag=A"},
		{"tag=A"},
	}

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithTagsOfNode(nodeTags[i]...),
		)
	}
	for i := 0; i < numReplicas; i++ {
		clusterState.AddReplica(
			int64(i),
			rf,
			allocator.WithTagsOfReplica(replicaTags[i]...),
		)
	}

	status, allocation := allocator.Solve(clusterState, allocator.WithTagMatching())
	require.True(t, status)

	clusterState.UpdateCurrentAssignment(allocation)

	const maxChurn = 1
	for index := 1; index < numNodes; index++ {
		clusterState.UpdateNode(int64(index), allocator.RemoveAllTagsOfNode())
	}

	status, allocation = allocator.Solve(
		clusterState,
		allocator.WithTagMatching(),
		allocator.WithChurnMinimized(),
		allocator.WithMaxChurn(maxChurn),
	)
	require.False(t, status)
	require.Nil(t, allocation)
}

// Premise : define replicas/nodes with respective demands/resources and ensure the load spread
// across resources is within some interval. In this case -> [ideal distribution * 0.8, ideal distribution * 1.2] (20% variance from ideal).
func TestQPSandDiskBalancing(t *testing.T) {
	const numReplicas = 12
	const rf = 1
	const numNodes = 6
	const nodeCapacity = 10_000
	sizeDemands := 0
	qpsDemands := 0

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithResourceOfNode(allocator.DiskResource, nodeCapacity),
		)
	}
	for i := 0; i < numReplicas; i++ {
		clusterState.AddReplica(
			int64(i),
			rf,
			allocator.WithDemandOfReplica(allocator.DiskResource, int64(i)),
			allocator.WithDemandOfReplica(allocator.QPS, int64(i)),
		)
		sizeDemands += i
		qpsDemands += i
	}

	status, allocation := allocator.Solve(clusterState, allocator.WithResources())
	require.True(t, status)
	reasonableVariance := 0.2
	idealSizeAllocation := float64(sizeDemands+qpsDemands) / float64(numNodes)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
	}
	nodeConsumption := make(map[int64]int64)
	for rID, nodeAssignments := range allocation {
		for _, nID := range nodeAssignments {
			nodeConsumption[nID] += 2 * int64(rID)
		}
	}
	for _, consumption := range nodeConsumption {
		require.True(t, (float64(consumption) >= (1-reasonableVariance)*idealSizeAllocation) && (float64(consumption) <= (1+reasonableVariance)*idealSizeAllocation))
	}
}

func isValidNodeAssignment(nodeIDs []int64, clusterSize int64) bool {
	for _, nodeID := range nodeIDs {
		if nodeID < 0 || nodeID > clusterSize {
			return false
		}
	}
	return true
}

func isEachReplicaAssignedToDifferentNode(nodeIDs []int64) bool {
	nodeIdSet := make(map[int64]struct{})
	for _, nodeID := range nodeIDs {
		if _, found := nodeIdSet[nodeID]; found {
			return false
		} else {
			nodeIdSet[nodeID] = struct{}{}
		}
	}
	return true
}

func nodeCapacityIsRespected(allocation map[int64][]int64, nodeCapacities []int64, replicaDemands []int64) bool {
	inUseCapacity := make(map[int64]int64)
	for replicaID, nodeAssignments := range allocation {
		for _, node := range nodeAssignments {
			inUseCapacity[node] += replicaDemands[replicaID]
		}
	}
	for nodeId, nodeCapacityConsumed := range inUseCapacity {
		if nodeCapacityConsumed > nodeCapacities[nodeId] {
			return false
		}
	}
	return true
}
