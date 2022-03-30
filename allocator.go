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

// allocator holds the shards, nodes, underlying CP-SAT solver, assigment variables, and Configuration needed.
type allocator struct {
	// ClusterState anonymous type that holds our shards and nodes metadata.
	*ClusterState
	// model is the underlying CP-SAT solver and the engine of this package.
	model *solver.Model
	// assignment represents variables that we constrain and impose on to satisfy allocation requirements.
	assignment map[shardId][]solver.IntVar
	// config holds allocation configurations -> {withCapacity, withTagAffinity...}
	config *Configuration
}

// newAllocator builds, configures, and returns an allocator from the necessary parameters.
func newAllocator(cs *ClusterState, config *Configuration) *allocator {
	model := solver.NewModel("Lé-allocator")
	assignment := make(map[shardId][]solver.IntVar)

	return &allocator{
		ClusterState: cs,
		model:        model,
		assignment:   assignment,
		config:       config,
	}
}

func Solve(cs *ClusterState, config *Configuration) (allocation Allocation, _ error) {
	if cs == nil {
		panic("ClusterState cannot be nil")
	}
	return newAllocator(cs, config).allocate()
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
	capacity := make([]solver.IntVar, 0)
	for _, re := range []Resource{DiskResource, QPS} {
		rawCapacity := int64(0)
		rawDemand := int64(0)
		for _, s := range a.shards {
			rawDemand += s.demands[re] * int64(a.config.rf)
		}
		// compute availability of node capacity. If not defined, assume we have just enough to
		// allocate the entire load on EACH node. This helps keep our bounds tight, as opposed to an arbitrary number.
		if c, ok := a.nodes[0].resources[re]; ok {
			rawCapacity = c
		} else {
			rawCapacity = rawDemand + 1
		}

		if rawCapacity*int64(len(a.nodes)) < rawDemand {
			return fmt.Errorf("sum of shard demands exceed sum of node resources available")
		}
		currResourceCap := a.model.NewIntVar(0, rawCapacity, fmt.Sprintf("IV used to minimize variance and enforce capacity constraint for Resource: %d", re))
		capacity = append(capacity, currResourceCap)
		tasks := make([]solver.Interval, 0)
		// demands represent the resource requirements placed on each node by potential matches to a shard.
		demands := make([]solver.IntVar, 0)
		for sId, nIds := range a.assignment {
			for i, id := range nIds {
				// go over shardIds and their respective ivs.
				// for that specific shard, tell the allocator "regardless of where you place this shard, you will
				// pay a cost of s.resource[re]". What we're asking the allocator to do is then arrange the intervals
				// in a fashion that does not violate our capacity requirements.
				toAdd := a.model.NewInterval(
					id,
					a.model.NewIntVarFromDomain(solver.NewDomain(1, int64(len(a.nodes))), "Adjusted intervals for upper bounds."),
					fixedSizedOneOffset,
					fmt.Sprintf("Interval representing demands placed on node by shard: %d, shard: %d", sId, i),
				)
				tasks = append(tasks, toAdd)
				demands = append(demands, a.model.NewConstant(a.shards[sId].demands[re], fmt.Sprintf("Demand for s.id:%d.", sId)))
			}
		}
		// set ceiling for interval interleaving.
		a.model.AddConstraints(
			solver.NewCumulativeConstraint(currResourceCap,
				tasks, demands,
			),
		)
		if a.config.withLoadBalancing {
			a.model.Minimize(solver.Sum(capacity...))
		}

	}
	return nil
}

func (a *allocator) adhereToNodeTags() error {
	shardIdsWithWaywardTags := make([]int64, 0)
	for sId, s := range a.shards {
		forbiddenAssignments := make([][]int64, 0)
		// for each shard-node pair, if incompatible, force the allocator to write-off said allocation.
		for nId, n := range a.nodes {
			if !shardTagsAreSubsetOfNodeTags(s.tags, n.tags) {
				forbiddenAssignments = append(forbiddenAssignments, []int64{int64(nId)})
			}
		}
		if len(forbiddenAssignments) == len(a.nodes) {
			shardIdsWithWaywardTags = append(shardIdsWithWaywardTags, int64(sId))
		}
		for i := 0; i < a.config.rf; i++ {
			a.model.AddConstraints(solver.NewForbiddenAssignmentsConstraint(
				[]solver.IntVar{a.assignment[sId][i]}, forbiddenAssignments,
			))
		}
	}
	if len(shardIdsWithWaywardTags) > 0 {
		return fmt.Errorf("tags that are absent on all nodes found on shard with shardId:%d ", shardIdsWithWaywardTags)
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

	for _, s := range a.shards {
		// go over shards, if a shard was previously assigned to some node, attempt to keep that assignment as long as
		// said node still exists in the cluster.
		if prevNodeIds, ok := a.currentAssignment[s.id]; ok {
			for i, iv := range a.assignment[s.id] {
				if _, ok := a.nodes[prevNodeIds[i]]; ok {
					newLiteral := a.model.NewLiteral(fmt.Sprintf("Literal tracking variance between assignment of shard:%d, shard:%d on node:%d", s.id, i, prevNodeIds[i]))
					a.model.AddConstraints(
						solver.NewLinearConstraint(
							solver.NewLinearExpr([]solver.IntVar{iv, a.model.NewConstant(int64(prevNodeIds[i]), fmt.Sprintf("IntVar corresponding to assignment of shard:%d, shard:%d on node:%d", s.id, i, prevNodeIds[i]))},
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

	if a.config.rf > len(a.nodes) {
		return nil, fmt.Errorf("rf specified in greater than cluster size")
	}

	// iterate over shards, assign each shardId a list of IV's sized rf.
	// These will ultimately then read as: shardId's shards assigned to nodes [N.1, N.2,...N.RF]
	for _, s := range a.shards {
		a.assignment[s.id] = make([]solver.IntVar, a.config.rf)
		for j := range a.assignment[s.id] {
			// constrain our IV's to live between [0, len(nodes) - 1].
			a.assignment[s.id][j] = a.model.NewIntVarFromDomain(
				solver.NewDomain(int64(a.nodes[0].id), int64(a.nodes[nodeId(len(a.nodes)-1)].id)),
				fmt.Sprintf("Allocation var for s.id:%d.", s.id))
		}
		a.model.AddConstraints(solver.NewAllDifferentConstraint(a.assignment[s.id]...))
	}
	// add constraints given configurations.
	if a.config.withCapacity {
		if err := a.adhereToResourcesAndBalance(); err != nil {
			return nil, err
		}
	}

	if a.config.withTagAffinity {
		if err := a.adhereToNodeTags(); err != nil {
			return nil, err
		}
	}

	for _, s := range a.assignment {
		a.model.AddConstraints(solver.NewAllDifferentConstraint(s...))
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
	for sId := range a.shards {
		nodes := a.assignment[sId]
		for _, n := range nodes {
			allocated := result.Value(n)
			res[int64(sId)] = append(res[int64(sId)], allocated)
		}
	}
	return res, nil
}
