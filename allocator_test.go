package allocator_test

import (
	"github.com/smcheema/allocator"
	"github.com/stretchr/testify/require"
	"log"
	"strconv"
	"testing"
	"time"
)

const benchmarkTimeout = time.Minute * 5

var result bool

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

// Premise : benchmark performance of replication only.
func BenchmarkReplication1K(b *testing.B)   { replicateAndAllocate(1, 10, 100, b) }
func BenchmarkReplication10K(b *testing.B)  { replicateAndAllocate(1, 100, 100, b) }
func BenchmarkReplication100K(b *testing.B) { replicateAndAllocate(1, 100, 1000, b) }
func BenchmarkReplication1M(b *testing.B)   { replicateAndAllocate(1, 1000, 1000, b) }

// Premise : benchmark performance of the disk-resource and replication.
func BenchmarkCapacity1K(b *testing.B)   { replicateWithCapacityAndAllocate(1, 10, 100, b) }
func BenchmarkCapacity10K(b *testing.B)  { replicateWithCapacityAndAllocate(1, 100, 100, b) }
func BenchmarkCapacity100K(b *testing.B) { replicateWithCapacityAndAllocate(1, 100, 1000, b) }
func BenchmarkCapacity1M(b *testing.B)   { replicateWithCapacityAndAllocate(1, 1000, 1000, b) }

// Premise : benchmark performance of the qps-resource and replication.
// this method is separate from the disk-resource benchmark due to extremely different
// benchmark performance.
func BenchmarkQps10(b *testing.B) { replicateWithQpsAndAllocate(1, 1, 10, b) }

// Premise : benchmark performance of tagging and replication.
func BenchmarkTagging1K(b *testing.B)  { replicateWithTaggingAndAllocate(1, 10, 100, b) }
func BenchmarkTagging10K(b *testing.B) { replicateWithTaggingAndAllocate(1, 100, 100, b) }

// Premise : benchmark performance of churn, for simplicity we currently set rf = 1.
func BenchmarkChurn1K(b *testing.B)   { replicateWithMaxChurnAndAllocate(10, 100, b) }
func BenchmarkChurn10K(b *testing.B)  { replicateWithMaxChurnAndAllocate(100, 100, b) }
func BenchmarkChurn100K(b *testing.B) { replicateWithMaxChurnAndAllocate(100, 1000, b) }

func replicateAndAllocate(rf int, numNodes int, numRanges int, b *testing.B) {
	validateRf(rf, numNodes)
	var status bool
	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i))
	}
	for i := 0; i < numRanges; i++ {
		clusterState.AddReplica(int64(i), rf)
	}
	for n := 0; n < b.N; n++ {
		status, _ = allocator.Solve(clusterState, allocator.WithTimeout(benchmarkTimeout))
	}
	result = status
	if !result {
		log.Print("benchmark allocation returned false, sus")
	}
}

func replicateWithCapacityAndAllocate(rf int, numNodes int, numRanges int, b *testing.B) {
	validateRf(rf, numNodes)
	var status bool
	nodeCapacity := 10000
	rangeDemand := 300
	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i),
			allocator.WithResourceOfNode(allocator.DiskResource, int64(nodeCapacity)))
	}
	for i := 0; i < numRanges; i++ {
		clusterState.AddReplica(int64(i), rf,
			allocator.WithDemandOfReplica(allocator.DiskResource, int64(rangeDemand)))
	}
	for n := 0; n < b.N; n++ {
		status, _ = allocator.Solve(clusterState, allocator.WithResources(), allocator.WithTimeout(benchmarkTimeout))
	}
	result = status
	if !result {
		log.Print("benchmark allocation returned false, sus")
	}
}

func replicateWithQpsAndAllocate(rf int, numNodes int, numRanges int, b *testing.B) {
	validateRf(rf, numNodes)
	var status bool
	rangeQps := 300
	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i))
	}
	for i := 0; i < numRanges; i++ {
		clusterState.AddReplica(int64(i), rf,
			allocator.WithDemandOfReplica(allocator.QPS, int64(rangeQps)))
	}
	for n := 0; n < b.N; n++ {
		status, _ = allocator.Solve(clusterState, allocator.WithResources(), allocator.WithTimeout(benchmarkTimeout))
	}
	result = status
	if !result {
		log.Print("benchmark allocation returned false, sus")
	}
}

func replicateWithTaggingAndAllocate(rf int, numNodes int, numRanges int, b *testing.B) {
	validateRf(rf, numNodes)
	var status bool
	// affineCount signifies the number of nodes that are affine to each shard.
	affineCount := 2
	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i), allocator.WithTagsOfNode(strconv.Itoa(i%affineCount)))
	}
	for i := 0; i < numRanges; i++ {
		clusterState.AddReplica(int64(i), rf, allocator.WithTagsOfReplica(strconv.Itoa(i%affineCount)))
	}
	for n := 0; n < b.N; n++ {
		status, _ = allocator.Solve(clusterState, allocator.WithTagMatching(), allocator.WithTimeout(benchmarkTimeout))
	}
	result = status
	if !result {
		log.Print("benchmark allocation returned false, sus")
	}
}

func replicateWithMaxChurnAndAllocate(numNodes int, numRanges int, b *testing.B) {
	const maxChurn = 2
	const rf = 1
	var status bool
	previousAllocation := make(allocator.Allocation)
	for r := 0; r < numRanges; r++ {
		previousAllocation[int64(r)] = append(previousAllocation[int64(r)], int64(r%numNodes))
	}
	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i))
	}
	for i := 0; i < numRanges; i++ {
		clusterState.AddReplica(int64(i), rf)
	}
	// force shard with id == 0 and shard with id == 1 to swap nodes.
	clusterState.UpdateReplica(int64(0), allocator.WithTagsOfReplica("x"))
	clusterState.UpdateReplica(int64(1), allocator.WithTagsOfReplica("y"))
	clusterState.UpdateNode(int64(0), allocator.WithTagsOfNode("y"))
	clusterState.UpdateNode(int64(1), allocator.WithTagsOfNode("x"))
	clusterState.UpdateCurrentAssignment(previousAllocation)
	for n := 0; n < b.N; n++ {
		status, _ = allocator.Solve(clusterState,
			allocator.WithMaxChurn(maxChurn),
			allocator.WithChurnMinimized(),
			allocator.WithTagMatching(),
			allocator.WithTimeout(benchmarkTimeout))
	}
	result = status
	if !result {
		log.Print("benchmark allocation returned false, sus")
	}
}

func validateRf(rf int, numNodes int) {
	if rf > numNodes {
		panic("rf greater than numNodes, i must panic")
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
