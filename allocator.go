package allocator

import (
	"fmt"
	"github.com/irfansharif/solver"
	"time"
)

// NodeID is a 64-bit identifier for nodes.
type NodeID int64

// RangeID is a 64-bit identifier for ranges.
type RangeID int64

// Resource is 32-bit identifier for resources -> {Disk, Qps}
type Resource int

// Allocation is the return type of our allocator.
// It models mappings of RangeIDs to a list of NodeIDs.
type Allocation map[RangeID][]NodeID

const (
	noMaxChurn            = -1
	DiskResource Resource = iota
	Qps
)

// Range encapsulates pertaining metadata for cockroachDB ranges.
type Range struct {
	// id represents a unique identifier.
	id RangeID
	// rf equals the replication factor of said range.
	rf int
	// tags are strings that showcase affinity for nodes.
	tags []string
	// demands model the resource requirements of said range.
	demands map[Resource]int64
}

// Node encapsulates pertaining metadata for cockroachDB nodes.
type Node struct {
	// id represents a unique identifier.
	id NodeID
	// tags are strings that showcase affinity for ranges.
	tags []string
	// resources model the resource profile of said node.
	resources map[Resource]int64
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
	prevAssignment map[RangeID][]NodeID
}

// Option manifests a closure that mutates allocation configurations in accordance with caller preferences.
type Option func(*options)

// Allocator holds the ranges, nodes, underlying CP-SAT solver, assigment variables, and configuration needed.
type Allocator struct {
	// ranges are a mapping of RangeID onto the Range struct.
	ranges map[RangeID]Range
	// nodes are a mapping of NodeID onto the Node struct.
	nodes map[NodeID]Node
	// model is the underlying CP-SAT solver and the engine of this package.
	model *solver.Model
	// assignment represents variables that we constrain and impose on to satisfy allocation requirements.
	assignment map[RangeID][]solver.IntVar
	// opts hold allocation configurations -> {withResources, withTagAffinity...}
	opts options
}

type NodeInterface struct{
	n Node
	NodeMap map[NodeID]Node
}

type RangeInterface struct {
	r        Range
	RangeMap map[RangeID]Range
}

type Cluster interface {
	ClusterWriter
}

type ClusterWriter interface{
	AddNode()
	RemoveNode () bool
	AddRange ()
	RemoveRange () bool
}

// AddNode Update or Add a new Node
func(ni NodeInterface) AddNode() {
	ni.NodeMap[ni.n.id] = ni.n
}

//Remove the node if its in the map
func(ni NodeInterface) RemoveNode() bool{

	if _, found := ni.NodeMap[ni.n.id]; found {
		fmt.Println("Removing Node ", ni.n.id)
		delete(ni.NodeMap,ni.n.id)
		return true
	}
	fmt.Println("Node not found ", ni.n.id)
	return true
}

func(ri RangeInterface) AddRange() {
	ri.RangeMap[ri.r.id] = ri.r
}

//Remove the range if its in the map
func(ri RangeInterface) RemoveRange() bool{

	if _, found := ri.RangeMap[ri.r.id]; found {
		fmt.Println("Removing Range ", ri.r.id)
		delete(ri.RangeMap,ri.r.id)
		return true
	}
	fmt.Println("Range not found ", ri.r.id)
	return true
}

func createRangeMap(ranges []Range) map[RangeID]Range{

	idToRangeMap := make(map[RangeID]Range)
	for _, r := range ranges {
		idToRangeMap[r.id] = r
	}

	return idToRangeMap
}

func createNodeMap(nodes []Node) map[NodeID]Node{

	idToNodeMap := make(map[NodeID]Node)
	for _, n := range nodes {
		idToNodeMap[n.id] = n
	}

	return idToNodeMap
}

// NewRange builds and returns ranges from the necessary parameters.
func NewRange(id RangeID, rf int, tags []string, demands map[Resource]int64) Range {
	return Range{
		id:      id,
		rf:      rf,
		tags:    tags,
		demands: demands,
	}
}

// NewNode builds and returns nodes from the necessary parameters.
func NewNode(id NodeID, tags []string, resources map[Resource]int64) Node {
	return Node{
		id:        id,
		tags:      tags,
		resources: resources,
	}
}

// New builds, configures, and returns an allocator from the necessary parameters.
func New(ranges []Range, nodes []Node, opts ...Option) *Allocator {
	model := solver.NewModel("Lé-Allocator")
	assignment := make(map[RangeID][]solver.IntVar)
	// iterate over ranges, assign each rangeID a list of IV's sized r.rf.
	// These will ultimately then read as: rangeID's replicas assigned to nodes [N.1, N.2,...N.RF]
	for _, r := range ranges {
		assignment[r.id] = make([]solver.IntVar, r.rf)
		for j := range assignment[r.id] {
			// constrain our IV's to live between [0, len(nodes) - 1].
			assignment[r.id][j] = model.NewIntVarFromDomain(
				solver.NewDomain(int64(nodes[0].id), int64(nodes[len(nodes)-1].id)),
				fmt.Sprintf("Allocation var for r.id:%d.", r.id))
		}
	}
	defaultOptions := options{}
	// assume no maxChurn initially, let the options slice override if needed.
	defaultOptions.maxChurn = noMaxChurn
	for _, opt := range opts {
		opt(&defaultOptions)
	}

	// build a convenience id-to-struct mapping for ranges.
	idToRangeMap := createRangeMap(ranges)

	// build a convenience id-to-struct mapping for nodes.
	idToNodeMap := createNodeMap(nodes)

	return &Allocator{
		ranges:     idToRangeMap,
		nodes:      idToNodeMap,
		model:      model,
		assignment: assignment,
		opts:       defaultOptions,
	}
}

// Print is a utility method that pretty-prints allocation information.
func (a Allocation) Print() {
	for rangeID, nodeIDs := range a {
		fmt.Println("Range with ID: ", rangeID, " on nodes: ", nodeIDs)
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
func WithPriorAssignment(prevAssignment map[RangeID][]NodeID) Option {
	return func(opt *options) {
		opt.prevAssignment = prevAssignment
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
	for _, re := range []Resource{DiskResource, Qps} {
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
func (a *Allocator) Allocate() (ok bool, allocation Allocation) {
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
			res[r.id] = append(res[r.id], NodeID(allocated))
		}
	}
	return true, res
}
