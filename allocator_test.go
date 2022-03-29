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

var result error

// Premise : test replication by requiring shards to be assigned to unique nodes.
func TestReplication(t *testing.T) {
	const numShards = 20
	const rf = 3
	const numNodes = 64

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i))
	}
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(int64(i))
	}

	configuration := allocator.NewConfiguration()

	allocation, err := allocator.Solve(clusterState, configuration)
	require.Nil(t, err)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
		require.True(t, isEachReplicaAssignedToDifferentNode(nodeAssignments))
	}
}

// Premise : test infeasible allocation by setting numNodes < rf. This is deemed infeasible since
// we mandate implicitly shards to live on separate nodes.
func TestReplicationWithInsufficientNodes(t *testing.T) {
	const numShards = 20
	const numNodes = 1

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i))
	}
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(int64(i))
	}

	configuration := allocator.NewConfiguration()

	allocation, err := allocator.Solve(clusterState, configuration)
	require.NotNil(t, err)
	require.Nil(t, allocation)
}

// Premise : Same as above.
func TestReplicationWithInfeasibleRF(t *testing.T) {
	const numShards = 20
	const rf = 128
	const numNodes = 64

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i))
	}
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(int64(i))
	}

	configuration := allocator.NewConfiguration(allocator.WithReplicationFactor(rf))

	allocation, err := allocator.Solve(clusterState, configuration)
	require.NotNil(t, err)
	require.Nil(t, allocation)
}

// Premise : build space-aware nodes and shards. Require all capacity constraints are respected.
func TestCapacity(t *testing.T) {
	const numShards = 20
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
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(
			int64(i),
			allocator.WithDemandOfShard(allocator.DiskResource, int64(i)),
		)
	}

	configuration := allocator.NewConfiguration(allocator.WithLoadBalancing(true), allocator.WithReplicationFactor(rf))

	newAllocation, err := allocator.Solve(clusterState, configuration)

	require.Nil(t, err)
	s := allocator.NewSerializer("test")
	s.WriteToFile(0, clusterState, configuration, newAllocation, 0)

	for _, nodeAssignments := range newAllocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
	}
}

// Premise : Same as above + replication.
func TestCapacityTogetherWithReplication(t *testing.T) {
	const numShards = 5
	const rf = 3
	const numNodes = 3
	clusterCapacities := []int64{90, 90, 90}
	shardSizeDemands := []int64{25, 10, 12, 11, 10}

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithResourceOfNode(allocator.DiskResource, clusterCapacities[i]),
		)
	}
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(
			int64(i),
			allocator.WithDemandOfShard(allocator.DiskResource, shardSizeDemands[i]),
		)
	}

	configuration := allocator.NewConfiguration(allocator.WithLoadBalancing(true))

	allocation, err := allocator.Solve(clusterState, configuration)
	require.Nil(t, err)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
		require.True(t, isEachReplicaAssignedToDifferentNode(nodeAssignments))
	}
	require.True(t, nodeCapacityIsRespected(allocation, clusterCapacities, shardSizeDemands))
}

// Premise : test unhappy path and ensure RF is accounted inside capacity computations.
func TestCapacityWithInfeasibleRF(t *testing.T) {
	const numShards = 5
	const rf = 5
	const numNodes = 3
	clusterCapacities := []int64{90, 90, 90}
	shardSizeDemands := []int64{25, 10, 12, 11, 10}

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithResourceOfNode(allocator.DiskResource, clusterCapacities[i]),
		)
	}
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(
			int64(i),
			allocator.WithDemandOfShard(allocator.DiskResource, shardSizeDemands[i]),
		)
	}

	configuration := allocator.NewConfiguration(allocator.WithLoadBalancing(true), allocator.WithReplicationFactor(rf))

	allocation, err := allocator.Solve(clusterState, configuration)
	require.NotNil(t, err)
	require.Nil(t, allocation)
}

// Premise : test unhappy path and ensure we are not allocating when impossible to do so.
func TestCapacityWithInsufficientNodes(t *testing.T) {
	const numShards = 10
	const rf = 1
	const numNodes = 3
	clusterCapacities := []int64{70, 70, 70}
	shardSizeDemands := [numShards]int64{85, 75, 12, 11, 10, 9, 8, 7, 6, 6}

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithResourceOfNode(allocator.DiskResource, clusterCapacities[i]),
		)
	}
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(
			int64(i),
			allocator.WithDemandOfShard(allocator.DiskResource, shardSizeDemands[i]),
		)
	}

	configuration := allocator.NewConfiguration(allocator.WithLoadBalancing(true), allocator.WithReplicationFactor(rf))

	allocation, err := allocator.Solve(clusterState, configuration)
	require.NotNil(t, err)
	require.Nil(t, allocation)
}

// Premise : check tag affinity works on small cluster and shard-set.
func TestTagsWithViableNodes(t *testing.T) {
	const numShards = 3
	const rf = 1
	const numNodes = 3
	nodeTags := [][]string{
		{"a=ant", "b=bus", "b=bin", "d=dog"},
		{"a=all", "b=bus", "e=eat", "f=fun"},
		{"a=art", "b=bin", "e=ear", "f=fur"},
	}
	shardTags := [][]string{
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
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(
			int64(i),
			allocator.WithTagsOfShard(shardTags[i]...),
		)
	}

	configuration := allocator.NewConfiguration(allocator.WithTagMatching(true), allocator.WithReplicationFactor(rf))

	expectedAllocation := allocator.Allocation{
		0: {2},
		1: {1},
		2: {0},
	}
	allocation, err := allocator.Solve(clusterState, configuration)
	require.Nil(t, err)
	require.Equal(t, expectedAllocation, allocation)
}

// Premise : validate failure upon orthogonal tag sets.
func TestTagsWithNonviableNodes(t *testing.T) {
	const numShards = 1
	const rf = 1
	const numNodes = 1
	nodeTags := [][]string{{"tag=A"}}
	shardTags := [][]string{{"tag=B"}}

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithTagsOfNode(nodeTags[i]...),
		)
	}
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(
			int64(i),
			allocator.WithTagsOfShard(shardTags[i]...),
		)
	}

	configuration := allocator.NewConfiguration(allocator.WithTagMatching(true), allocator.WithReplicationFactor(rf))

	allocation, err := allocator.Solve(clusterState, configuration)
	require.NotNil(t, err)
	require.Nil(t, allocation)
}

// Premise : allocate once, force allocator to modify prior allocation due to modified tags, ensure impossible to do
// so due to low maxChurn limit.
func TestMaxChurnWithInfeasibleLimit(t *testing.T) {
	const numShards = 3
	const numNodes = 6
	nodeTags := [][]string{
		{"tag=A"},
		{"tag=A"},
		{"tag=A"},
		{"tag=A"},
		{"tag=A"},
		{"tag=A"},
	}
	shardTags := [][]string{
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
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(
			int64(i),
			allocator.WithTagsOfShard(shardTags[i]...),
		)
	}

	configuration := allocator.NewConfiguration(allocator.WithTagMatching(true))

	allocation, err := allocator.Solve(clusterState, configuration)
	require.Nil(t, err)

	clusterState.UpdateCurrentAssignment(allocation)

	for index := 1; index < numNodes; index++ {
		clusterState.UpdateNode(int64(index), allocator.RemoveAllTagsOfNode())
	}

	const maxChurn = 1
	configuration.UpdateConfiguration(
		allocator.WithChurnMinimized(true),
		allocator.WithMaxChurn(maxChurn),
	)

	allocation, err = allocator.Solve(clusterState, configuration)
	require.NotNil(t, err)
	require.Nil(t, allocation)
}

// Premise : define shards/nodes with disk demands/resources and ensure the load spread
// across resources is within some interval. In this case -> [ideal distribution * 0.7, ideal distribution * 1.3] (30% variance from ideal).
func TestDiskBalancing(t *testing.T) {
	const numShards = 20
	const rf = 2
	const numNodes = 8
	const nodeCapacity = 10_000
	const scalingFactor = 50

	sizeDemands := 0

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithResourceOfNode(allocator.DiskResource, nodeCapacity),
		)
	}
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(
			int64(i),
			allocator.WithDemandOfShard(allocator.DiskResource, scalingFactor*int64(i)),
		)
		sizeDemands += scalingFactor * i * rf
	}

	configuration := allocator.NewConfiguration(allocator.WithResources(true), allocator.WithReplicationFactor(rf))

	allocation, err := allocator.Solve(clusterState, configuration)
	require.Nil(t, err)
	reasonableVariance := 0.2
	idealSizeAllocation := float64(sizeDemands) / float64(numNodes)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
	}
	nodeConsumption := make(map[int64]int64)
	for sId, nodeAssignments := range allocation {
		for _, nId := range nodeAssignments {
			nodeConsumption[nId] += scalingFactor * int64(sId)
		}
	}
	for _, consumption := range nodeConsumption {
		require.True(t, (float64(consumption) >= (1-reasonableVariance)*idealSizeAllocation) && (float64(consumption) <= (1+reasonableVariance)*idealSizeAllocation))
	}
}

// Premise : define shards/nodes with respective demands/resources and ensure the load spread
// across resources is within some interval. In this case -> [ideal distribution * 0.8, ideal distribution * 1.2] (20% variance from ideal).
func TestQPSandDiskBalancing(t *testing.T) {
	const numShards = 12
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
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(
			int64(i),
			allocator.WithDemandOfShard(allocator.DiskResource, int64(i)),
			allocator.WithDemandOfShard(allocator.QPS, int64(i)),
		)
		sizeDemands += i
		qpsDemands += i
	}

	configuration := allocator.NewConfiguration(allocator.WithLoadBalancing(true), allocator.WithReplicationFactor(rf))

	allocation, err := allocator.Solve(clusterState, configuration)
	require.Nil(t, err)
	reasonableVariance := 0.2
	idealSizeAllocation := float64(sizeDemands+qpsDemands) / float64(numNodes)
	for _, nodeAssignments := range allocation {
		require.Equal(t, len(nodeAssignments), rf)
		require.True(t, isValidNodeAssignment(nodeAssignments, numNodes))
	}
	nodeConsumption := make(map[int64]int64)
	for sId, nodeAssignments := range allocation {
		for _, nId := range nodeAssignments {
			nodeConsumption[nId] += 2 * int64(sId)
		}
	}
	for _, consumption := range nodeConsumption {
		require.True(t, (float64(consumption) >= (1-reasonableVariance)*idealSizeAllocation) && (float64(consumption) <= (1+reasonableVariance)*idealSizeAllocation))
	}
}

func TestQpsMultipleT(t *testing.T) {
	println("lol man")
	const numShards = 30
	const rf = 3
	const numNodes = 10
	const nodeCapacity = 9_500
	qpsDemands := 0
	tStep := 0

	shardsQpsDemands := [30]int64{414, 1755, 1677, 1681, 699, 1219, 1758, 388, 1454, 1289, 850, 1444, 291, 1040, 1040, 1280, 1006, 1591, 480, 494, 303, 957, 1682, 701, 559, 1261, 1063, 1002, 1035, 902}

	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(
			int64(i),
			allocator.WithResourceOfNode(allocator.QPS, nodeCapacity),
		)
	}
	for i := 0; i < numShards; i++ {
		clusterState.AddShard(
			int64(i),
			allocator.WithDemandOfShard(allocator.QPS, shardsQpsDemands[i]),
		)
		qpsDemands += i
	}

	configuration := allocator.NewConfiguration(allocator.WithCapacity(true), allocator.WithReplicationFactor(rf), allocator.WithChurnMinimized(true), allocator.WithTimeout(time.Minute))

	s := allocator.NewSerializer("qps")
	allocateAndMeasure := func() {
		start := time.Now()
		allocation, err := allocator.Solve(clusterState, configuration)
		duration := time.Since(start)
		require.Nil(t, err)

		s.WriteToFile(tStep, clusterState, configuration, allocation, int(duration.Milliseconds()))
		tStep++

		clusterState.UpdateCurrentAssignment(allocation)
	}
	allocateAndMeasure()

	configuration.UpdateConfiguration(allocator.WithLoadBalancing(true), allocator.WithChurnMinimized(false))
	allocateAndMeasure()
}

// Benchmark names are suffixed with _rRF_nN_sS --> reads as: this benchmark is run with RF = f, numNodes = n, numShard = s.
// Premise : benchmark performance of replication only.
func BenchmarkReplication_1RF_10N_100S(b *testing.B)    { replicateAndAllocate(1, 10, 100, b) }
func BenchmarkReplication_1RF_100N_100S(b *testing.B)   { replicateAndAllocate(1, 100, 100, b) }
func BenchmarkReplication_1RF_100N_1000S(b *testing.B)  { replicateAndAllocate(1, 100, 1000, b) }
func BenchmarkReplication_1RF_1000N_1000S(b *testing.B) { replicateAndAllocate(1, 1000, 1000, b) }

// Premise : benchmark performance of the disk-resource and replication.
func BenchmarkCapacity_1RF_10N_100S(b *testing.B)  { replicateWithCapacityAndAllocate(1, 10, 100, b) }
func BenchmarkCapacity_1RF_100N_100S(b *testing.B) { replicateWithCapacityAndAllocate(1, 100, 100, b) }
func BenchmarkCapacity_1RF_100N_1000S(b *testing.B) {
	replicateWithCapacityAndAllocate(1, 100, 1000, b)
}
func BenchmarkCapacity_1RF_1000N_1000S(b *testing.B) {
	replicateWithCapacityAndAllocate(1, 1000, 1000, b)
}

// Premise : benchmark performance of the qps-resource and replication.
// this method is separate from the disk-resource benchmark due to extremely different
// benchmark performance.
func BenchmarkQps_1RF_1N_10S(b *testing.B) { replicateWithQpsAndAllocate(1, 1, 10, b) }

// Premise : benchmark performance of tagging and replication.
func BenchmarkTagging_1RF_10N_100S(b *testing.B)  { replicateWithTaggingAndAllocate(1, 10, 100, b) }
func BenchmarkTagging_1RF_100N_100S(b *testing.B) { replicateWithTaggingAndAllocate(1, 100, 100, b) }

// Premise : benchmark performance of churn, for simplicity we currently set rf = 1.
func BenchmarkChurn_10N_100S(b *testing.B)   { replicateWithMaxChurnAndAllocate(10, 100, b) }
func BenchmarkChurn_100N_100S(b *testing.B)  { replicateWithMaxChurnAndAllocate(100, 100, b) }
func BenchmarkChurn_100N_1000S(b *testing.B) { replicateWithMaxChurnAndAllocate(100, 1000, b) }

func replicateAndAllocate(rf int, numNodes int, numRanges int, b *testing.B) {
	validateRf(rf, numNodes)
	var err error
	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i))
	}
	for i := 0; i < numRanges; i++ {
		clusterState.AddShard(int64(i))
	}

	configuration := allocator.NewConfiguration(allocator.WithTimeout(benchmarkTimeout), allocator.WithReplicationFactor(rf))

	for n := 0; n < b.N; n++ {
		_, err = allocator.Solve(clusterState, configuration)
	}
	result = err
	if result != nil {
		log.Print(result)
	}
}

func replicateWithCapacityAndAllocate(rf int, numNodes int, numRanges int, b *testing.B) {
	validateRf(rf, numNodes)
	var err error
	nodeCapacity := 10000
	shardDemand := 300
	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i),
			allocator.WithResourceOfNode(allocator.DiskResource, int64(nodeCapacity)))
	}
	for i := 0; i < numRanges; i++ {
		clusterState.AddShard(int64(i))
		allocator.WithDemandOfShard(allocator.DiskResource, int64(shardDemand))
	}

	configuration := allocator.NewConfiguration(
		allocator.WithLoadBalancing(true),
		allocator.WithTimeout(benchmarkTimeout),
		allocator.WithReplicationFactor(rf),
	)

	for n := 0; n < b.N; n++ {
		_, err = allocator.Solve(clusterState, configuration)
	}
	result = err
	if result != nil {
		log.Print(result)
	}
}

func replicateWithQpsAndAllocate(rf int, numNodes int, numRanges int, b *testing.B) {
	validateRf(rf, numNodes)
	var err error
	shardQps := 300
	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i))
	}
	for i := 0; i < numRanges; i++ {
		clusterState.AddShard(int64(i),
			allocator.WithDemandOfShard(allocator.QPS, int64(shardQps)))
	}

	configuration := allocator.NewConfiguration(
		allocator.WithLoadBalancing(true),
		allocator.WithTimeout(benchmarkTimeout),
		allocator.WithReplicationFactor(rf),
	)

	for n := 0; n < b.N; n++ {
		_, err = allocator.Solve(clusterState, configuration)
	}
	result = err
	if result != nil {
		log.Print(result)
	}
}

func replicateWithTaggingAndAllocate(rf int, numNodes int, numRanges int, b *testing.B) {
	validateRf(rf, numNodes)
	var err error
	// affineCount signifies the number of nodes that are affine to each shard.
	affineCount := 2
	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i), allocator.WithTagsOfNode(strconv.Itoa(i%affineCount)))
	}
	for i := 0; i < numRanges; i++ {
		clusterState.AddShard(int64(i), allocator.WithTagsOfShard(strconv.Itoa(i%affineCount)))
	}

	configuration := allocator.NewConfiguration(
		allocator.WithTagMatching(true),
		allocator.WithTimeout(benchmarkTimeout),
		allocator.WithReplicationFactor(rf),
	)

	for n := 0; n < b.N; n++ {
		_, err = allocator.Solve(clusterState, configuration)
	}
	result = err
	if result != nil {
		log.Print(result)
	}
}

func replicateWithMaxChurnAndAllocate(numNodes int, numRanges int, b *testing.B) {
	const maxChurn = 2
	const rf = 1
	var err error
	previousAllocation := make(allocator.Allocation)
	for r := 0; r < numRanges; r++ {
		previousAllocation[int64(r)] = append(previousAllocation[int64(r)], int64(r%numNodes))
	}
	clusterState := allocator.NewClusterState()
	for i := 0; i < numNodes; i++ {
		clusterState.AddNode(int64(i))
	}
	for i := 0; i < numRanges; i++ {
		clusterState.AddShard(int64(i))
	}
	// force shard with id == 0 and shard with id == 1 to swap nodes.
	clusterState.UpdateShard(int64(0), allocator.WithTagsOfShard("x"))
	clusterState.UpdateShard(int64(1), allocator.WithTagsOfShard("y"))
	clusterState.UpdateNode(int64(0), allocator.WithTagsOfNode("y"))
	clusterState.UpdateNode(int64(1), allocator.WithTagsOfNode("x"))
	clusterState.UpdateCurrentAssignment(previousAllocation)

	configuration := allocator.NewConfiguration(
		allocator.WithMaxChurn(maxChurn),
		allocator.WithChurnMinimized(true),
		allocator.WithTagMatching(true),
		allocator.WithTimeout(benchmarkTimeout),
		allocator.WithReplicationFactor(rf),
	)

	for n := 0; n < b.N; n++ {
		_, err = allocator.Solve(clusterState, configuration)
	}
	result = err
	if result != nil {
		log.Print(result)
	}
}

func validateRf(rf int, numNodes int) {
	if rf > numNodes {
		panic("rf greater than numNodes, i must panic")
	}
}

func isValidNodeAssignment(nodeIds []int64, clusterSize int64) bool {
	for _, nId := range nodeIds {
		if nId < 0 || nId > clusterSize {
			return false
		}
	}
	return true
}

func isEachReplicaAssignedToDifferentNode(nodeIds []int64) bool {
	nodeIdSet := make(map[int64]struct{})
	for _, nId := range nodeIds {
		if _, found := nodeIdSet[nId]; found {
			return false
		} else {
			nodeIdSet[nId] = struct{}{}
		}
	}
	return true
}

func nodeCapacityIsRespected(allocation map[int64][]int64, nodeCapacities []int64, shardDemands []int64) bool {
	inUseCapacity := make(map[int64]int64)
	for sId, nodeAssignments := range allocation {
		for _, node := range nodeAssignments {
			inUseCapacity[node] += shardDemands[sId]
		}
	}
	for nodeId, nodeCapacityConsumed := range inUseCapacity {
		if nodeCapacityConsumed > nodeCapacities[nodeId] {
			return false
		}
	}
	return true
}
