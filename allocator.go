package allocator

import (
	"fmt"
	"github.com/irfansharif/or-tools/cpsatsolver"
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

type options struct {
	withNodeCapacity bool
	withTagAffinity  bool
}

type Option func(*options)

type Allocator struct {
	ranges     []Range
	nodes      []Node
	model      *cpsatsolver.Model
	assignment map[RangeID]map[NodeID]cpsatsolver.Literal
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
	model := cpsatsolver.NewModel()
	assignment := make(map[RangeID]map[NodeID]cpsatsolver.Literal)
	for _, r := range ranges {
		assignment[r.id] = make(map[NodeID]cpsatsolver.Literal, len(nodes))
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

func (a *Allocator) rangeLiterals(r Range) []cpsatsolver.Literal {
	var res []cpsatsolver.Literal
	ns := a.assignment[r.id]
	for _, k := range ns {
		res = append(res, k)
	}
	return res
}

func (a *Allocator) literal(r Range, n Node) cpsatsolver.Literal {
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

func (a *Allocator) adhereToNodeResources() {
	for _, re := range []Resource{DiskResource} {
		for _, n := range a.nodes {
			capacity := n.resources[re]

			var vars []cpsatsolver.IntVar
			var coefficients []int64
			for _, r := range a.ranges {
				vars = append(vars, a.literal(r, n))
				coefficients = append(coefficients, r.demands[re])
			}

			a.model.AddConstraints(cpsatsolver.NewLinearConstraint(
				cpsatsolver.NewLinearExpr(vars, coefficients, 0),
				cpsatsolver.NewDomain(0, capacity)))
		}
	}
}

func (a *Allocator) adhereToNodeTags() {
	for _, r := range a.ranges {
		unAssignableNodes := make([]cpsatsolver.Literal, 0, len(a.nodes))
		for _, n := range a.nodes {
			if !rangeTagsAreSubsetOfNodeTags(r.tags, n.tags) {
				unAssignableNodes = append(unAssignableNodes, a.literal(r, n))
			}
		}
		a.model.AddConstraints(
			cpsatsolver.NewExactlyKConstraint(0, unAssignableNodes...),
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

func (a *Allocator) Allocate() (ok bool, assignments map[RangeID][]NodeID) {
	if a.opts.withNodeCapacity {
		a.adhereToNodeResources()
	}

	if a.opts.withTagAffinity {
		a.adhereToNodeTags()
	}

	for _, r := range a.ranges {
		a.model.AddConstraints(cpsatsolver.NewExactlyKConstraint(r.rf, a.rangeLiterals(r)...))
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
