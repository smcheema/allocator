package allocator

import (
	"fmt"
	"github.com/irfansharif/solver"
	"math"
)

type NodeID int64
type RangeID int64
type Resource int

const (
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

type churnOptions struct {
	withMinimalChurn bool
	withMaxChurn     bool
	maxChurn         int64
	prevAssignment   map[RangeID]map[NodeID]int
}

type options struct {
	withNodeCapacity bool
	withTagAffinity  bool
	churnOptions     churnOptions
}

type Option func(*options)

type Allocator struct {
	ranges     []Range
	nodes      []Node
	model      *solver.Model
	assignment map[RangeID]map[NodeID]solver.Literal
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
	assignment := make(map[RangeID]map[NodeID]solver.Literal)
	for _, r := range ranges {
		assignment[r.id] = make(map[NodeID]solver.Literal, len(nodes))
		for _, n := range nodes {
			assignment[r.id][n.id] = model.NewLiteral(fmt.Sprintf("r%d-on-n%d", r.id, n.id))
		}
	}
	defaultOptions := options{}
	for _, opt := range opts {
		opt(&defaultOptions)
	}

	return &Allocator{
		ranges:     ranges,
		nodes:      nodes,
		model:      model,
		assignment: assignment,
		opts:       defaultOptions,
	}
}

func (a *Allocator) rangeLiterals(r Range) []solver.Literal {
	var res []solver.Literal
	ns := a.assignment[r.id]
	for _, k := range ns {
		res = append(res, k)
	}
	return res
}

func (a *Allocator) literal(r Range, n Node) solver.Literal {
	return a.assignment[r.id][n.id]
}

func WithNodeCapacity() Option {
	return func(opt *options) {
		opt.withNodeCapacity = true
	}
}

func WithTagMatching() Option {
	return func(opt *options) {
		opt.withTagAffinity = true
	}
}

// WithChurnConstraint to add churn constraints
// If maxChurn == math.MaxInt64 then it will not apply the maxChurn constraint
// If minimizeChurn == true it will try to minimize the churn
func WithChurnConstraint(prevAssignment map[RangeID][]NodeID, minimizeChurn bool, maxChurn int64) Option {
	return func(opt *options) {
		opt.churnOptions.prevAssignment = toMap(prevAssignment)
		opt.churnOptions.withMinimalChurn = minimizeChurn
		opt.churnOptions.withMaxChurn = maxChurn < math.MaxInt64
		if maxChurn < 0 {
			panic("max-churn must be greater than or equal to 0")
		}
		opt.churnOptions.maxChurn = maxChurn
	}
}

func (a *Allocator) adhereToNodeResources() {
	for _, re := range []Resource{DiskResource} {
		for _, n := range a.nodes {
			capacity := n.resources[re]

			var vars []solver.IntVar
			var coefficients []int64
			for _, r := range a.ranges {
				vars = append(vars, a.literal(r, n))
				coefficients = append(coefficients, r.demands[re])
			}

			a.model.AddConstraints(solver.NewLinearConstraint(
				solver.NewLinearExpr(vars, coefficients, 0),
				solver.NewDomain(0, capacity)))
		}
	}
}

func (a *Allocator) adhereToNodeTags() {
	for _, r := range a.ranges {
		unAssignableNodes := make([]solver.Literal, 0, len(a.nodes))
		for _, n := range a.nodes {
			if !rangeTagsAreSubsetOfNodeTags(r.tags, n.tags) {
				unAssignableNodes = append(unAssignableNodes, a.literal(r, n))
			}
		}
		a.model.AddConstraints(
			solver.NewExactlyKConstraint(0, unAssignableNodes...),
		)
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

func toMap(t map[RangeID][]NodeID) map[RangeID]map[NodeID]int {
	ret := make(map[RangeID]map[NodeID]int)
	for k, v := range t {
		ret[k] = make(map[NodeID]int)
		for _, n := range v {
			ret[k][n] = 1
		}
	}
	return ret
}

func (a *Allocator) Allocate() (ok bool, assignments map[RangeID][]NodeID) {
	if a.opts.withNodeCapacity {
		a.adhereToNodeResources()
	}

	if a.opts.withTagAffinity {
		a.adhereToNodeTags()
	}

	for _, r := range a.ranges {
		a.model.AddConstraints(solver.NewExactlyKConstraint(r.rf, a.rangeLiterals(r)...))
	}

	if a.opts.churnOptions.withMinimalChurn || a.opts.churnOptions.withMaxChurn {
		a.adhereToChurnConstraint()
	}

	result := a.model.Solve()
	if result.Infeasible() || result.Invalid() {
		return false, nil
	}

	res := make(map[RangeID][]NodeID)
	for _, r := range a.ranges {
		for _, n := range a.nodes {
			allocated := result.BooleanValue(a.literal(r, n))
			if allocated {
				res[r.id] = append(res[r.id], n.id)
			}
		}
	}
	return true, res
}

// Minimize the number of Assignments which were 1 but would now become 0
// Note: If a Range or a Node is no longer available and that causes an existing allocation to be removed,
// it will not count as a churn
func (a *Allocator) adhereToChurnConstraint() {

	var toMinimizeTheSumLiterals = make([]solver.Literal, 0, len(a.ranges)*len(a.nodes))
	prevAssignmentMap := a.opts.churnOptions.prevAssignment

	for _, r := range a.ranges {
		for _, n := range a.nodes {
			if previousAssignment, ok := prevAssignmentMap[r.id][n.id]; ok == true && previousAssignment == 1 {
				// Literal below would exist in the map because we are iterating through the new ranges and nodes
				newLiteral := a.literal(r, n)
				// previousAssignment = 1 and newLiteral.Not() would be true if the new value is 0. Minimize the sum of these
				toMinimizeTheSumLiterals = append(toMinimizeTheSumLiterals, newLiteral.Not())
			}
		}
	}

	if a.opts.churnOptions.withMinimalChurn {
		a.model.Minimize(solver.Sum(solver.AsIntVars(toMinimizeTheSumLiterals)...))
	}
	if a.opts.churnOptions.withMaxChurn {
		a.model.AddConstraints(
			solver.NewAtMostKConstraint(int(a.opts.churnOptions.maxChurn), toMinimizeTheSumLiterals...),
		)
	}
}
