package allocator

import (
	"fmt"
	"github.com/irfansharif/solver"
	"log"
	"strings"
)

// Allocation is the return type of our allocator.
// It models mappings of shardIds to a list of nodeIds.
type Allocation map[int64][]int64

// allocator holds the shards, nodes, underlying CP-SAT solver, assigment variables, and configuration needed.
type allocator struct {
	// ClusterState anonymous type that holds our shards and nodes metadata.
	*ClusterState
	// model is the underlying CP-SAT solver and the engine of this package.
	model *solver.Model
	// assignment represents variables that we constrain and impose on to satisfy allocation requirements.
	assignment map[shardId][]solver.IntVar
	// config holds allocation configurations -> {withResources, withTagAffinity...}
	config configuration
}

// newAllocator builds, configures, and returns an allocator from the necessary parameters.
func newAllocator(cs *ClusterState, opts ...Option) *allocator {
	model := solver.NewModel("Lé-allocator")
	assignment := make(map[shardId][]solver.IntVar)
	defaultConfiguration := configuration{
		// assume no maxChurn initially, let the opts slice override if needed.
		maxChurn:      noMaxChurn,
		searchTimeout: defaultTimeout,
	}

	for _, opt := range opts {
		opt(&defaultConfiguration)
	}

	return &allocator{
		ClusterState: cs,
		model:        model,
		assignment:   assignment,
		config:       defaultConfiguration,
	}
}

func Solve(cs *ClusterState, opts ...Option) (allocation Allocation, _ error) {
	if cs == nil {
		panic("ClusterState cannot be nil")
	}
	return newAllocator(cs, opts...).allocate()
}

// Print is a utility method that pretty-prints allocation information.
func (a Allocation) Print() {
	for sId, nIds := range a {
		fmt.Println("shard with Id: ", sId, " on nodes: ", nIds)
	}
}

func (a *allocator) adhereToResourcesAndBalance() error {
	// build a fixed offset of size one initially to avoid polluting the constant set with unnecessary variables.
	// we can use this across loop iterations, since this is used only to indicate the distance between intervals starts + ends.
	fixedSizedOneOffset := a.model.NewConstant(1, fmt.Sprintf("Fixed offset of size 1."))
	for _, re := range []Resource{DiskResource, QPS} {
		rawCapacity := int64(0)
		rawDemand := int64(0)
		for _, r := range a.shards {
			rawDemand += r.demands[re] * int64(r.rf)
		}
		// compute availability of node capacity. If not defined, assume we have just enough to
		// allocate the entire load on EACH node. This helps keep our bounds tight, as opposed to an arbitrary number.
		if c, ok := a.nodes[0].resources[re]; ok {
			rawCapacity = c
		} else {
			rawCapacity = rawDemand
		}

		if rawCapacity*int64(len(a.nodes)) < rawDemand {
			return fmt.Errorf("sum of range demands exceed sum of node resources available")
		}
		capacity := a.model.NewIntVar(0, rawCapacity, fmt.Sprintf("IV used to minimize variance and enforce capacity constraint for Resource: %d", re))
		tasks := make([]solver.Interval, 0)
		// demands represent the resource requirements placed on each node by potential matches to a shard.
		demands := make([]solver.IntVar, 0)
		for rId, nIds := range a.assignment {
			for i, id := range nIds {
				// go over shardIds and their respective ivs.
				// for that specific shard, tell the allocator "regardless of where you place this shard, you will
				// pay a cost of r.resource[re]". What we're asking the allocator to do is then arrange the intervals
				// in a fashion that does not violate our capacity requirements.
				toAdd := a.model.NewInterval(
					id,
					a.model.NewIntVarFromDomain(solver.NewDomain(1, int64(len(a.nodes))), "Adjusted intervals for upper bounds."),
					fixedSizedOneOffset,
					fmt.Sprintf("Interval representing demands placed on node by shard: %d, shard: %d", rId, i),
				)
				tasks = append(tasks, toAdd)
				demands = append(demands, a.model.NewConstant(a.shards[rId].demands[re], fmt.Sprintf("Demand for r.id:%d.", rId)))
			}
		}
		// set ceiling for interval interleaving.
		a.model.AddConstraints(
			solver.NewCumulativeConstraint(capacity,
				tasks, demands,
			),
		)
		a.model.Minimize(solver.Sum(capacity))
	}
	return nil
}

func (a *allocator) adhereToNodeTags() error {
	rangeIdsWithWaywardTags := make([]int64, 0)
	for rId, r := range a.shards {
		forbiddenAssignments := make([][]int64, 0)
		// for each shard-node pair, if incompatible, force the allocator to write-off said allocation.
		for nId, n := range a.nodes {
			if !shardTagsAreSubsetOfNodeTags(r.tags, n.tags) {
				forbiddenAssignments = append(forbiddenAssignments, []int64{int64(nId)})
			}
		}
		if len(forbiddenAssignments) == len(a.nodes) {
			rangeIdsWithWaywardTags = append(rangeIdsWithWaywardTags, int64(rId))
		}
		for i := 0; i < r.rf; i++ {
			a.model.AddConstraints(solver.NewForbiddenAssignmentsConstraint(
				[]solver.IntVar{a.assignment[rId][i]}, forbiddenAssignments,
			))
		}
	}
	if len(rangeIdsWithWaywardTags) > 0 {
		return fmt.Errorf("tags that are absent on all nodes found on ranges with rangeId:%d ", rangeIdsWithWaywardTags)
	}
	return nil
}

// shardTagsAreSubsetOfNodeTags returns true iff a shard's tags are a subset of a node's tags
func shardTagsAreSubsetOfNodeTags(shardTags map[string]struct{}, nodeTags map[string]struct{}) bool {
	for sTag := range shardTags {
		if _, found := nodeTags[sTag]; !found {
			return false
		}
	}
	return true
}

func (a *allocator) adhereToChurnConstraint() {
	if a.currentAssignment == nil {
		panic("missing/invalid prior assignment")
	}
	toMinimizeTheSumLiterals := make([]solver.Literal, 0)
	fixedDomain := solver.NewDomain(0, 0)

	for _, r := range a.shards {
		// go over shards, if a shard was previously assigned to some node, attempt to keep that assignment as long as
		// said node still exists in the cluster.
		if prevNodeIds, ok := a.currentAssignment[r.id]; ok {
			for i, iv := range a.assignment[r.id] {
				if _, ok := a.nodes[prevNodeIds[i]]; ok {
					newLiteral := a.model.NewLiteral(fmt.Sprintf("Literal tracking variance between assignment of shard:%d, shard:%d on node:%d", r.id, i, prevNodeIds[i]))
					a.model.AddConstraints(
						solver.NewLinearConstraint(
							solver.NewLinearExpr([]solver.IntVar{iv, a.model.NewConstant(int64(prevNodeIds[i]), fmt.Sprintf("IntVar corresponding to assignment of shard:%d, shard:%d on node:%d", r.id, i, prevNodeIds[i]))},
								[]int64{1, -1}, 0), fixedDomain).OnlyEnforceIf(newLiteral))
					toMinimizeTheSumLiterals = append(toMinimizeTheSumLiterals, newLiteral.Not())
				}
			}
		}
	}

	// minimize variance/churn.
	if a.config.withMinimalChurn {
		a.model.Minimize(solver.Sum(solver.AsIntVars(toMinimizeTheSumLiterals)...))
	}

	// we use the following inequality to deem if maxChurn was set, if so, constrain.
	if a.config.maxChurn != noMaxChurn {
		a.model.AddConstraints(
			solver.NewAtMostKConstraint(int(a.config.maxChurn), toMinimizeTheSumLiterals...),
		)
	}
}

// allocate is a terminal method call that returns a status and paired allocation.
// The status could be false if the existing model is invalid or unsatisfiable.
func (a *allocator) allocate() (allocation Allocation, err error) {

	// iterate over shards, assign each shardId a list of IV's sized r.rf.
	// These will ultimately then read as: shardId's shards assigned to nodes [N.1, N.2,...N.RF]
	rangeIdsWithInfeasibleRF := make([]int64, 0)
	for _, r := range a.shards {
		if r.rf > len(a.nodes) {
			rangeIdsWithInfeasibleRF = append(rangeIdsWithInfeasibleRF, int64(r.id))
		}
		a.assignment[r.id] = make([]solver.IntVar, r.rf)
		for j := range a.assignment[r.id] {
			// constrain our IV's to live between [0, len(nodes) - 1].
			a.assignment[r.id][j] = a.model.NewIntVarFromDomain(
				solver.NewDomain(int64(a.nodes[0].id), int64(a.nodes[nodeId(len(a.nodes)-1)].id)),
				fmt.Sprintf("Allocation var for r.id:%d.", r.id))
		}
		a.model.AddConstraints(solver.NewAllDifferentConstraint(a.assignment[r.id]...))
	}
	if len(rangeIdsWithInfeasibleRF) > 0 {
		return nil, fmt.Errorf("rf passed in greater than cluster size for rangeId: %d", rangeIdsWithInfeasibleRF)
	}
	// add constraints given configurations.
	if a.config.withResources {
		if err := a.adhereToResourcesAndBalance(); err != nil {
			return nil, err
		}
	}

	if a.config.withTagAffinity {
		if err := a.adhereToNodeTags(); err != nil {
			return nil, err
		}
	}

	for _, r := range a.assignment {
		a.model.AddConstraints(solver.NewAllDifferentConstraint(r...))
	}

	if a.config.withMinimalChurn || a.config.maxChurn != noMaxChurn {
		a.adhereToChurnConstraint()
	}

	ok, err := a.model.Validate()
	if !ok {
		log.Println("invalid model built: ", err)
		return nil, fmt.Errorf("invalid model generated, inspect logs for further insights")
	}

	var result solver.Result
	if a.config.verboseLogging {
		var sb strings.Builder
		result = a.model.Solve(solver.WithLogger(&sb, loggingPrefix), solver.WithTimeout(a.config.searchTimeout))
		log.Print(sb.String())
	} else {
		result = a.model.Solve(solver.WithTimeout(a.config.searchTimeout))
	}

	if !(result.Feasible() || result.Optimal()) {
		return nil, fmt.Errorf("allocation failed, most likely due to a timeout or an infeasible model, use VerboseLogging to debug")
	}

	res := make(Allocation)
	for rId := range a.shards {
		nodes := a.assignment[rId]
		for _, n := range nodes {
			allocated := result.Value(n)
			res[int64(rId)] = append(res[int64(rId)], allocated)
		}
	}
	return res, nil
}
