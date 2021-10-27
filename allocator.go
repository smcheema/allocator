package allocator

import (
	"fmt"
	"github.com/irfansharif/solver"
)

type NodeID int64
type RangeID int64
type Resource int
type Allocation map[RangeID][]NodeID

const (
	noMaxChurn            = -1
	DiskResource Resource = iota
)

type Range struct {
	id      RangeID
	rf      int
	tags    []string
	demands map[Resource]int64
}

type Node struct {
	id        NodeID
	tags      []string
	resources map[Resource]int64
}

type options struct {
	withResources    bool
	withTagAffinity  bool
	withMinimalChurn bool
	maxChurn         int64
	prevAssignment   map[RangeID][]NodeID
}

type Option func(*options)

type Allocator struct {
	ranges     map[RangeID]Range
	nodes      map[NodeID]Node
	model      *solver.Model
	assignment map[RangeID][]solver.IntVar
	opts       options
}

func NewRange(id RangeID, rf int, tags []string, demands map[Resource]int64) Range {
	return Range{
		id:      id,
		rf:      rf,
		tags:    tags,
		demands: demands,
	}
}

func NewNode(id NodeID, tags []string, resources map[Resource]int64) Node {
	return Node{
		id:        id,
		tags:      tags,
		resources: resources,
	}
}

func New(ranges []Range, nodes []Node, opts ...Option) *Allocator {
	model := solver.NewModel("Lé-Allocator")
	assignment := make(map[RangeID][]solver.IntVar)
	for _, r := range ranges {
		assignment[r.id] = make([]solver.IntVar, r.rf)
		for j := range assignment[r.id] {
			assignment[r.id][j] = model.NewIntVarFromDomain(
				solver.NewDomain(int64(nodes[0].id), int64(nodes[len(nodes)-1].id)),
				fmt.Sprintf("Allocation var for r.id:%d.", r.id))
		}
	}
	defaultOptions := options{}
	defaultOptions.maxChurn = noMaxChurn
	for _, opt := range opts {
		opt(&defaultOptions)
	}

	idToRangeMap := make(map[RangeID]Range)
	for _, r := range ranges {
		idToRangeMap[r.id] = r
	}

	idToNodeMap := make(map[NodeID]Node)
	for _, n := range nodes {
		idToNodeMap[n.id] = n
	}

	return &Allocator{
		ranges:     idToRangeMap,
		nodes:      idToNodeMap,
		model:      model,
		assignment: assignment,
		opts:       defaultOptions,
	}
}

func (a Allocation) Print() {
	for rangeID, nodeIDs := range a {
		fmt.Println("Range with ID: ", rangeID, " on nodes: ", nodeIDs)
	}
}

func (a *Allocator) rangeIntVars(r Range) []solver.IntVar {
	var res []solver.IntVar
	ns := a.assignment[r.id]
	for _, k := range ns {
		res = append(res, k)
	}
	return res
}

func WithNodeCapacity() Option {
	return func(opt *options) {
		opt.withResources = true
	}
}

func WithTagMatching() Option {
	return func(opt *options) {
		opt.withTagAffinity = true
	}
}

func WithPriorAssignment(prevAssignment map[RangeID][]NodeID) Option {
	return func(opt *options) {
		opt.prevAssignment = prevAssignment
	}
}

func WithMaxChurn(maxChurn int64) Option {
	return func(opt *options) {
		if maxChurn < 0 {
			panic("max-churn must be greater than or equal to 0")
		}
		opt.maxChurn = maxChurn
	}
}

func WithChurnMinimized() Option {
	return func(opt *options) {
		opt.withMinimalChurn = true
	}
}

func (a *Allocator) adhereToNodeResources() {
	fixedSizedOneOffset := a.model.NewConstant(1, fmt.Sprintf("Fixed offset of size 1."))
	for _, re := range []Resource{DiskResource} {
		capacity := a.model.NewConstant(a.nodes[0].resources[re], fmt.Sprintf("Fixed constant used to enforce capacity constraint for resource: %d", re))
		tasks := make([]solver.Interval, 0)
		demands := make([]solver.IntVar, 0)
		for rID, nIDs := range a.assignment {
			for i, id := range nIDs {
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
		a.model.AddConstraints(
			solver.NewCumulativeConstraint(capacity,
				tasks, demands,
			),
		)
	}
}

func (a *Allocator) adhereToNodeTags() {
	for rID, r := range a.ranges {
		forbiddenAssignments := make([][]int64, 0)
		for nID, n := range a.nodes {
			if !rangeTagsAreSubsetOfNodeTags(r.tags, n.tags) {
				forbiddenAssignments = append(forbiddenAssignments, []int64{int64(nID)})
			}
		}
		a.model.AddConstraints(solver.NewForbiddenAssignmentsConstraint(
			a.assignment[rID], forbiddenAssignments,
		))
	}
}

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
		if prevNodeIDs, ok := a.opts.prevAssignment[r.id]; ok {
			for i, iv := range a.assignment[r.id] {
				newLiteral := a.model.NewLiteral(fmt.Sprintf("Literal tracking variance between assignment of range:%d, replica:%d on node:%d", r.id, i, prevNodeIDs[i]))
				a.model.AddConstraints(
					solver.NewLinearConstraint(
						solver.NewLinearExpr([]solver.IntVar{iv, a.model.NewConstant(int64(prevNodeIDs[i]), fmt.Sprintf("IntVar corresponding to assignment of range:%d, replica:%d on node:%d", r.id, i, prevNodeIDs[i]))},
							[]int64{1, -1}, 0), fixedDomain).OnlyEnforceIf(newLiteral))
				toMinimizeTheSumLiterals = append(toMinimizeTheSumLiterals, newLiteral.Not())
			}
		}
	}

	if a.opts.withMinimalChurn {
		a.model.Minimize(solver.Sum(solver.AsIntVars(toMinimizeTheSumLiterals)...))
	}

	if a.opts.maxChurn != noMaxChurn {
		a.model.AddConstraints(
			solver.NewAtMostKConstraint(int(a.opts.maxChurn), toMinimizeTheSumLiterals...),
		)
	}
}

func (a *Allocator) Allocate() (ok bool, allocation Allocation) {
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

	result := a.model.Solve()
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
