package allocator

import (
	"fmt"
	"github.com/irfansharif/solver"
	"log"
	"strings"
	"time"
)

// Allocation is the return type of our allocator.
// It models mappings of ReplicaIDs to a list of NodeIDs.
type Allocation map[int64][]int64

const (
	noMaxChurn              = -1
	defaultTimeout          = time.Second * 10
	loggingPrefix           = ""
)

// allocOptions hold runtime allocation configurations.
type allocOptions struct {
	// withResources signals the allocator to perform balancing and capacity checking.
	withResources bool
	// withTagAffinity forces the allocator to perform affine allocations only.
	withTagAffinity bool
	// withMinimalChurn asks the allocator to reduce variance from a prior allocation.
	withMinimalChurn bool
	// maxChurn limits the number of moves needed to fulfill an allocation request with respect to a prior allocation.
	maxChurn int64
	// searchTimeout forces the solver to return within the specified duration.
	searchTimeout time.Duration
	// verboseLogging routes all the internal solver logs to stdout.
	verboseLogging bool
}

// AllocOption manifests a closure that mutates allocation configurations in accordance with caller preferences.
type AllocOption func(*allocOptions)

// allocator holds the replicas, nodes, underlying CP-SAT solver, assigment variables, and configuration needed.
type allocator struct {
	*ClusterState
	// model is the underlying CP-SAT solver and the engine of this package.
	model *solver.Model
	// assignment represents variables that we constrain and impose on to satisfy allocation requirements.
	assignment map[replicaId][]solver.IntVar
	// opts hold allocation configurations -> {withResources, withTagAffinity...}
	opts allocOptions
}

// newAllocator builds, configures, and returns an allocator from the necessary parameters.
// Note this allocator should not be reused carefully after solving a problem
func newAllocator(cs *ClusterState, opts ...AllocOption) *allocator {
	model := solver.NewModel("Lé-allocator")
	assignment := make(map[replicaId][]solver.IntVar)
	defaultOptions := allocOptions{
		// assume no maxChurn initially, let the allocOptions slice override if needed.
		maxChurn: noMaxChurn,
		searchTimeout: defaultTimeout,
	}

	for _, opt := range opts {
		opt(&defaultOptions)
	}

	return &allocator{
		ClusterState: cs,
		model:        model,
		assignment:   assignment,
		opts:         defaultOptions,
	}
}

func Solve(cs *ClusterState, opts ...AllocOption) (ok bool, allocation Allocation) {
	allocator := newAllocator(cs, opts...)
	return allocator.allocate()
}

// Print is a utility method that pretty-prints allocation information.
func (a Allocation) Print() {
	for replicaID, nodeIDs := range a {
		fmt.Println("replica with ID: ", replicaID, " on nodes: ", nodeIDs)
	}
}

// WithResources is a closure that configures the allocator to adhere to capacity constraints and load-balance across
// resources.
func WithResources() AllocOption {
	return func(opt *allocOptions) {
		opt.withResources = true
	}
}

// WithTagMatching is a closure that configures the allocator to perform affine allocations only.
func WithTagMatching() AllocOption {
	return func(opt *allocOptions) {
		opt.withTagAffinity = true
	}
}

// WithMaxChurn is a closure that inspects and sets a hard limit on the maximum number of moves deviating
// from some prior assignment.
func WithMaxChurn(maxChurn int64) AllocOption {
	return func(opt *allocOptions) {
		if maxChurn < 0 {
			panic("max-churn must be greater than or equal to 0")
		}
		opt.maxChurn = maxChurn
	}
}

// WithChurnMinimized is a closure that configures the allocator to minimize variance from some prior allocation.
func WithChurnMinimized() AllocOption {
	return func(opt *allocOptions) {
		opt.withMinimalChurn = true
	}
}

// WithTimeout is a closure that configures the allocator to conclude its search within the duration provided.
func WithTimeout(searchTimeout time.Duration) AllocOption {
	return func(opt *allocOptions) {
		opt.searchTimeout = searchTimeout
	}
}

// WithVerboseLogging is a closure that forces our solver to expose its logs to the caller for inspection.
func WithVerboseLogging() AllocOption {
	return func(opt *allocOptions) {
		opt.verboseLogging = true
	}
}

func (a *allocator) adhereToResourcesAndBalance() {
	// build a fixed offset of size one initially to avoid polluting the constant set with unnecessary variables.
	// we can use this across loop iterations, since this is used only to indicate the distance between intervals starts + ends.
	fixedSizedOneOffset := a.model.NewConstant(1, fmt.Sprintf("Fixed offset of size 1."))
	for _, re := range []Resource{DiskResource, QPS} {
		rawCapacity := int64(0)
		// compute availability of node capacity. If not defined, assume we have just enough to
		// allocate the entire load on EACH node. This helps keep our bounds tight, as opposed to an arbitrary number.
		if c, ok := a.nodes[0].resources[re]; ok {
			rawCapacity = c
		} else {
			for _, r := range a.replicas {
				rawCapacity += r.demands[re]
			}
		}
		capacity := a.model.NewIntVar(0, rawCapacity, fmt.Sprintf("IV used to minimize variance and enforce capacity constraint for Resource: %d", re))
		tasks := make([]solver.Interval, 0)
		// demands represent the resource requirements placed on each node by potential matches to a replica.
		demands := make([]solver.IntVar, 0)
		for rID, nIDs := range a.assignment {
			for i, id := range nIDs {
				// go over replicaIDs and their respective ivs.
				// for that specific replica, tell the allocator "regardless of where you place this replica, you will
				// pay a cost of r.resource[re]". What we're asking the allocator to do is then arrange the intervals
				// in a fashion that does not violate our capacity requirements.
				toAdd := a.model.NewInterval(
					id,
					a.model.NewIntVarFromDomain(solver.NewDomain(1, int64(len(a.nodes))), "Adjusted intervals for upper bounds."),
					fixedSizedOneOffset,
					fmt.Sprintf("Interval representing demands placed on node by replica: %d, replica: %d", rID, i),
				)
				tasks = append(tasks, toAdd)
				demands = append(demands, a.model.NewConstant(a.replicas[rID].demands[re], fmt.Sprintf("Demand for r.id:%d.", rID)))
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
}

func (a *allocator) adhereToNodeTags() {
	for rID, r := range a.replicas {
		forbiddenAssignments := make([][]int64, 0)
		// for each replica-node pair, if incompatible, force the allocator to write-off said allocation.
		for nID, n := range a.nodes {
			if !replicaTagsAreSubsetOfNodeTags(r.tags, n.tags) {
				forbiddenAssignments = append(forbiddenAssignments, []int64{int64(nID)})
			}
		}
		for i := 0; i < r.rf; i++ {
			a.model.AddConstraints(solver.NewForbiddenAssignmentsConstraint(
				[]solver.IntVar{a.assignment[rID][i]}, forbiddenAssignments,
			))
		}
	}
}

// replicaTagsAreSubsetOfNodeTags returns true iff a replica's tags are a subset of a node's tags
func replicaTagsAreSubsetOfNodeTags(replicaTags []string, nodeTags []string) bool {
	for _, replicaTag := range replicaTags {
		foundMatch := false
		for _, nodeTag := range nodeTags {
			if replicaTag == nodeTag {
				foundMatch = true
				break
			}
		}
		if !foundMatch {
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

	for _, r := range a.replicas {
		// go over replicas, if a replica was previously assigned to some node, attempt to keep that assignment as long as
		// said node still exists in the cluster.
		if prevNodeIDs, ok := a.currentAssignment[r.id]; ok {
			for i, iv := range a.assignment[r.id] {
				if _, ok := a.nodes[prevNodeIDs[i]]; ok {
					newLiteral := a.model.NewLiteral(fmt.Sprintf("Literal tracking variance between assignment of replica:%d, replica:%d on node:%d", r.id, i, prevNodeIDs[i]))
					a.model.AddConstraints(
						solver.NewLinearConstraint(
							solver.NewLinearExpr([]solver.IntVar{iv, a.model.NewConstant(int64(prevNodeIDs[i]), fmt.Sprintf("IntVar corresponding to assignment of replica:%d, replica:%d on node:%d", r.id, i, prevNodeIDs[i]))},
								[]int64{1, -1}, 0), fixedDomain).OnlyEnforceIf(newLiteral))
					toMinimizeTheSumLiterals = append(toMinimizeTheSumLiterals, newLiteral.Not())
				}
			}
		}
	}

	// minimize variance/churn.
	if a.opts.withMinimalChurn {
		a.model.Minimize(solver.Sum(solver.AsIntVars(toMinimizeTheSumLiterals)...))
	}

	// we use the following inequality to deem if maxChurn was set, if so, constrain.
	if a.opts.maxChurn != noMaxChurn {
		a.model.AddConstraints(
			solver.NewAtMostKConstraint(int(a.opts.maxChurn), toMinimizeTheSumLiterals...),
		)
	}
}

// allocate is a terminal method call that returns a status and paired allocation.
// The status could be false if the existing model is invalid or unsatisfiable.
func (a *allocator) allocate() (ok bool, allocation Allocation) {

	// iterate over replicas, assign each replicaID a list of IV's sized r.rf.
	// These will ultimately then read as: replicaID's replicas assigned to nodes [N.1, N.2,...N.RF]
	for _, r := range a.replicas {
		a.assignment[r.id] = make([]solver.IntVar, r.rf)
		for j := range a.assignment[r.id] {
			// constrain our IV's to live between [0, len(nodes) - 1].
			a.assignment[r.id][j] = a.model.NewIntVarFromDomain(
				solver.NewDomain(int64(a.nodes[0].id), int64(a.nodes[nodeId(len(a.nodes)-1)].id)),
				fmt.Sprintf("Allocation var for r.id:%d.", r.id))
		}
	}
	// add constraints given opts/configurations.
	if a.opts.withResources {
		a.adhereToResourcesAndBalance()
	}

	if a.opts.withTagAffinity {
		a.adhereToNodeTags()
	}

	for _, r := range a.assignment {
		a.model.AddConstraints(solver.NewAllDifferentConstraint(r...))
	}

	if a.opts.withMinimalChurn || a.opts.maxChurn != noMaxChurn {
		a.adhereToChurnConstraint()
	}

	ok, err := a.model.Validate()
	if !ok {
		fmt.Println(err)
	}

	var result solver.Result
	if a.opts.verboseLogging {
		var sb strings.Builder
		result = a.model.Solve(solver.WithLogger(&sb, loggingPrefix), solver.WithTimeout(a.opts.searchTimeout))
		log.Print(sb.String())
	} else {
		result = a.model.Solve(solver.WithTimeout(a.opts.searchTimeout))
	}

	if !(result.Feasible() || result.Optimal()) {
		return false, nil
	}

	res := make(Allocation)
	for rID, r := range a.replicas {
		nodes := a.assignment[rID]
		for _, n := range nodes {
			allocated := result.Value(n)
			res[int64(r.id)] = append(res[int64(r.id)], allocated)
		}
	}
	return true, res
}
