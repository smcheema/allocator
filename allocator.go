package allocator

import (
	"fmt"
	"github.com/irfansharif/solver"
	"time"
)

// nodeId is a 64-bit identifier for nodes.
type nodeId int64

// rangeId is a 64-bit identifier for ranges.
type rangeId int64

// resource is 32-bit identifier for resources -> {disk, qps}
type resource int

// Allocation is the return type of our allocator.
// It models mappings of RangeIDs to a list of NodeIDs.
type Allocation map[int64][]int64

const (
	noMaxChurn            = -1
	diskResource resource = iota
	qps
)

// ckRange encapsulates pertaining metadata for cockroachDB ranges.
type ckRange struct {
	// id represents a unique identifier.
	id rangeId
	// rf equals the replication factor of said range.
	rf int
	// tags are strings that showcase affinity for nodes.
	tags []string
	// demands model the resource requirements of said range.
	demands map[resource]int64
}

// node encapsulates pertaining metadata for cockroachDB nodes.
type node struct {
	// id represents a unique identifier.
	id nodeId
	// tags are strings that showcase affinity for ranges.
	tags []string
	// resources model the resource profile of said node.
	resources map[resource]int64
}

// options hold runtime allocation configurations.
type options struct {
	// withResources signals the allocator to perform balancing and capacity checking.
	withResources bool
	// withTagAffinity forces the allocator to perform affine allocations only.
	withTagAffinity bool
	// withMinimalChurn asks the allocator to reduce variance from a prior allocation.
	withMinimalChurn bool
	// maxChurn limits the number of moves needed to fulfill an allocation request with respect to a prior allocation.
	maxChurn int64
	// prevAssignment holds some prior allocation.
	prevAssignment map[rangeId][]nodeId
}

// Option manifests a closure that mutates allocation configurations in accordance with caller preferences.
type Option func(*options)

// Allocator holds the ranges, nodes, underlying CP-SAT solver, assigment variables, and configuration needed.
type Allocator struct {
	// ranges are a mapping of rangeId onto the ckRange struct.
	ranges map[rangeId]ckRange
	// nodes are a mapping of nodeId onto the node struct.
	nodes map[nodeId]node
	// model is the underlying CP-SAT solver and the engine of this package.
	model *solver.Model
	// assignment represents variables that we constrain and impose on to satisfy allocation requirements.
	assignment map[rangeId][]solver.IntVar
	// opts hold allocation configurations -> {withResources, withTagAffinity...}
	opts options
}

type Cluster struct {
	nodes map[nodeId]node
}
type Items struct {
	ranges map[rangeId]ckRange
}

func NewCluster() Cluster {
	return Cluster{
		nodes: make(map[nodeId]node),
	}
}

func NewItems() Items {
	return Items{
		ranges: make(map[rangeId]ckRange),
	}
}

// AddNode adds a node into the cluster collection.
func (c Cluster) AddNode(id int64, nodeCapacity int64, tags ...string) {
	c.nodes[nodeId(id)] = node{
		id:        nodeId(id),
		tags:      tags,
		resources: map[resource]int64{diskResource: nodeCapacity},
	}
}

// UpdateNodeTags updates the tags for a node currently residing in the cluster collection,
// returning true for a successful update, false if the nodeID does not map to any node.
func (c Cluster) UpdateNodeTags(id int64, tags ...string) bool {
	if n, found := c.nodes[nodeId(id)]; found {
		n.tags = tags
		return true
	}
	return false
}

// UpdateNodeCapacity updates the disk-capacity for a node currently residing in the cluster collection,
// returning true for a successful update, false if the nodeID does not map to any node.
func (c Cluster) UpdateNodeCapacity(id int64, nodeCapacity int64) bool {
	if n, found := c.nodes[nodeId(id)]; found {
		n.resources[diskResource] = nodeCapacity
		return true
	}
	return false
}

// RemoveNode removes a given node from the cluster collection, returning true is the deletion is successful,
// false if the nodeID does not map to any node.
func (c Cluster) RemoveNode(id int64) bool {
	if _, found := c.nodes[nodeId(id)]; found {
		delete(c.nodes, nodeId(id))
		return true
	}
	return false
}

// AddRange adds a range into the items' collection.
func (i Items) AddRange(id int64, rf int, diskDemand int64, q int64, tags ...string) {
	i.ranges[rangeId(id)] = ckRange{
		id:      rangeId(id),
		rf:      rf,
		tags:    tags,
		demands: map[resource]int64{diskResource: diskDemand, qps: q},
	}
}

// RemoveRange removes a given range from the items' collection, returning true is the deletion is successful,
// false if the rangeID does not map to any range.
func (i Items) RemoveRange(id int64) bool {
	if _, found := i.ranges[rangeId(id)]; found {
		delete(i.ranges, rangeId(id))
		return true
	}
	return false
}

// UpdateRangeTags updates the tags for a range currently residing in the items' collection,
// returning true for a successful update, false if the rangeID does not map to any range.
func (i Items) UpdateRangeTags(id int64, tags ...string) bool {
	if r, found := i.ranges[rangeId(id)]; found {
		r.tags = tags
		return true
	}
	return false
}

// UpdateRangeDiskDemand updates the disk-demand for a range currently residing in the items' collection,
// returning true for a successful update, false if the rangeID does not map to any range.
func (i Items) UpdateRangeDiskDemand(id rangeId, diskDemand int64) bool {
	if r, found := i.ranges[id]; found {
		r.demands[diskResource] = diskDemand
		return true
	}
	return false
}

// UpdateRangeQps updates the qps for a range currently residing in the items' collection,
// returning true for a successful update, false if the rangeID does not map to any range.
func (i Items) UpdateRangeQps(id int64, q int64) bool {
	if r, found := i.ranges[rangeId(id)]; found {
		r.demands[qps] = q
		return true
	}
	return false
}

// New builds, configures, and returns an allocator from the necessary parameters.
func New(opts ...Option) *Allocator {
	model := solver.NewModel("Lé-Allocator")
	assignment := make(map[rangeId][]solver.IntVar)
	defaultOptions := options{}
	// assume no maxChurn initially, let the options slice override if needed.
	defaultOptions.maxChurn = noMaxChurn
	for _, opt := range opts {
		opt(&defaultOptions)
	}

	return &Allocator{
		model:      model,
		assignment: assignment,
		opts:       defaultOptions,
	}
}

// Print is a utility method that pretty-prints allocation information.
func (a Allocation) Print() {
	for rangeID, nodeIDs := range a {
		fmt.Println("ckRange with ID: ", rangeID, " on nodes: ", nodeIDs)
	}
}

// WithNodeCapacity is a closure that configures the allocator to adhere to capacity constraints and load-balance across
// resources.
func WithNodeCapacity() Option {
	return func(opt *options) {
		opt.withResources = true
	}
}

// WithTagMatching is a closure that configures the allocator to perform affine allocations only.
func WithTagMatching() Option {
	return func(opt *options) {
		opt.withTagAffinity = true
	}
}

// WithPriorAssignment is a closure that ingests a prior allocation.
func WithPriorAssignment(prevAssignment map[int64][]int64) Option {
	temp := make(map[rangeId][]nodeId)
	for r, n := range prevAssignment {
		temp[rangeId(r)] = make([]nodeId, len(n))
		for nid := range n {
			temp[rangeId(r)] = append(temp[rangeId(r)], nodeId(nid))
		}
	}
	return func(opt *options) {
		opt.prevAssignment = temp
	}
}

// WithMaxChurn is a closure that inspects and sets a hard limit on the maximum number of moves deviating
// from some prior assignment.
func WithMaxChurn(maxChurn int64) Option {
	return func(opt *options) {
		if maxChurn < 0 {
			panic("max-churn must be greater than or equal to 0")
		}
		opt.maxChurn = maxChurn
	}
}

// WithChurnMinimized is a closure that configures the allocator to minimize variance from some prior allocation.
func WithChurnMinimized() Option {
	return func(opt *options) {
		opt.withMinimalChurn = true
	}
}

func (a *Allocator) adhereToNodeResources() {
	// build a fixed offset of size one initially to avoid polluting the constant set with unnecessary variables.
	// we can use this across loop iterations, since this is used only to indicate the distance between intervals starts + ends.
	fixedSizedOneOffset := a.model.NewConstant(1, fmt.Sprintf("Fixed offset of size 1."))
	for _, re := range []resource{diskResource, qps} {
		rawCapacity := int64(0)
		// compute availability of node capacity. If not defined, assume we have just enough to
		// allocate the entire load on EACH node. This helps keep our bounds tight, as opposed to an arbitrary number.
		if c, ok := a.nodes[0].resources[re]; ok {
			rawCapacity = c
		} else {
			for _, r := range a.ranges {
				rawCapacity += r.demands[re]
			}
		}
		capacity := a.model.NewIntVar(0, rawCapacity, fmt.Sprintf("IV used to minimize variance and enforce capacity constraint for resource: %d", re))
		tasks := make([]solver.Interval, 0)
		// demands represent the resource requirements placed on each node by potential matches to a range.
		demands := make([]solver.IntVar, 0)
		for rID, nIDs := range a.assignment {
			for i, id := range nIDs {
				// go over rangeIDs and their respective ivs.
				// for that specific range, tell the allocator "regardless of where you place this replica, you will
				// pay a cost of r.resource[re]". What we're asking the allocator to do is then arrange the intervals
				// in a fashion that does not violate our capacity requirements.
				toAdd := a.model.NewInterval(
					id,
					a.model.NewIntVarFromDomain(solver.NewDomain(1, int64(len(a.nodes))), "Adjusted intervals for upper bounds."),
					fixedSizedOneOffset,
					fmt.Sprintf("Interval representing demands placed on node by range: %d, replica: %d", rID, i),
				)
				tasks = append(tasks, toAdd)
				demands = append(demands, a.model.NewConstant(a.ranges[rID].demands[re], fmt.Sprintf("Demand for r.id:%d.", rID)))
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

func (a *Allocator) adhereToNodeTags() {
	for rID, r := range a.ranges {
		forbiddenAssignments := make([][]int64, 0)
		// for each range-node pair, if incompatible, force the allocator to write-off said allocation.
		for nID, n := range a.nodes {
			if !rangeTagsAreSubsetOfNodeTags(r.tags, n.tags) {
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

// rangeTagsAreSubsetOfNodeTags returns true iff a range's tags are a subset of a node's tags
func rangeTagsAreSubsetOfNodeTags(rangeTags []string, nodeTags []string) bool {
	for _, rangeTag := range rangeTags {
		foundMatch := false
		for _, nodeTag := range nodeTags {
			if rangeTag == nodeTag {
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

func (a *Allocator) adhereToChurnConstraint() {
	if a.opts.prevAssignment == nil {
		panic("missing/invalid prior assignment")
	}
	toMinimizeTheSumLiterals := make([]solver.Literal, 0)
	fixedDomain := solver.NewDomain(0, 0)

	for _, r := range a.ranges {
		// go over ranges, if a range was previously assigned to some node, attempt to keep that assignment as long as
		// said node still exists in the cluster.
		if prevNodeIDs, ok := a.opts.prevAssignment[r.id]; ok {
			for i, iv := range a.assignment[r.id] {
				if _, ok := a.nodes[prevNodeIDs[i]]; ok {
					newLiteral := a.model.NewLiteral(fmt.Sprintf("Literal tracking variance between assignment of range:%d, replica:%d on node:%d", r.id, i, prevNodeIDs[i]))
					a.model.AddConstraints(
						solver.NewLinearConstraint(
							solver.NewLinearExpr([]solver.IntVar{iv, a.model.NewConstant(int64(prevNodeIDs[i]), fmt.Sprintf("IntVar corresponding to assignment of range:%d, replica:%d on node:%d", r.id, i, prevNodeIDs[i]))},
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

// Allocate is a terminal method call that returns a status and paired allocation.
// The status could be false if the existing model is invalid or unsatisfiable.
func (a *Allocator) Allocate(cluster Cluster, items Items) (ok bool, allocation Allocation) {
	a.nodes = cluster.nodes
	a.ranges = items.ranges
	// iterate over ranges, assign each rangeID a list of IV's sized r.rf.
	// These will ultimately then read as: rangeID's replicas assigned to nodes [N.1, N.2,...N.RF]
	for _, r := range a.ranges {
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
		a.adhereToNodeResources()
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

	// set a hard time limit of 10s on our solver.
	result := a.model.Solve(solver.WithTimeout(time.Second * 10))
	if result.Infeasible() || result.Invalid() {
		return false, nil
	}

	res := make(Allocation)
	for rID, r := range a.ranges {
		nodes := a.assignment[rID]
		for _, n := range nodes {
			allocated := result.Value(n)
			res[int64(r.id)] = append(res[int64(r.id)], allocated)
		}
	}
	return true, res
}
