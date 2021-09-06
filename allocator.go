package allocator

import (
	"fmt"
	"github.com/irfansharif/or-tools/cpsatsolver"
)

type NodeID int64
type RangeID int64
type Resource int

type ConstraintOption func(*Allocator)

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

type Allocator struct {
	ranges     []Range
	nodes      []Node
	model      *cpsatsolver.Model
	assignment map[RangeID]map[NodeID]cpsatsolver.Literal
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

func New(ranges []Range, nodes []Node) *Allocator {
	model := cpsatsolver.NewModel()
	assignment := make(map[RangeID]map[NodeID]cpsatsolver.Literal)
	for _, r := range ranges {
		assignment[r.id] = make(map[NodeID]cpsatsolver.Literal, len(nodes))
		for _, n := range nodes {
			assignment[r.id][n.id] = model.NewLiteral(fmt.Sprintf("r%d-on-n%d", r.id, n.id))
		}
	}

	return &Allocator{
		ranges:     ranges,
		nodes:      nodes,
		model:      model,
		assignment: assignment,
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

func WithNodeCapacityConstraint() ConstraintOption {
	return func(al *Allocator) {
		al.adhereToNodeResourcesConstraint()
	}
}

func WithTagMatchingConstraint() ConstraintOption {
	return func(al *Allocator) {
		al.adhereToNodeTagsConstraint()
	}
}

func (a *Allocator) adhereToNodeResourcesConstraint() {
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

func (a *Allocator) adhereToNodeTagsConstraint() {
	for _, r := range a.ranges {
		nodeThatAreUnAssignable := make([]cpsatsolver.Literal, 0, len(a.nodes))
		for _, n := range a.nodes {
			if !rangeTagsAreSubsetOfNodeTags(r.tags, n.tags) {
				nodeThatAreUnAssignable = append(nodeThatAreUnAssignable, a.literal(r, n))
			}
		}
		a.model.AddConstraints(
			cpsatsolver.NewAllowedLiteralAssignmentsConstraint(
				nodeThatAreUnAssignable,
				[][]bool{
					make([]bool, len(nodeThatAreUnAssignable)),
				}),
		)
	}
}

func rangeTagsAreSubsetOfNodeTags(tOne []string, tTwo []string) bool {
	for _, rangeTag := range tOne {
		for tagIndex, nodeTag := range tTwo {
			if rangeTag == nodeTag {
				break
			} else if tagIndex == len(tTwo)-1 {
				return false
			}
		}
	}
	return true
}

func (a *Allocator) Allocate(constraintOptions ...ConstraintOption) (ok bool, assignments map[RangeID][]NodeID) {
	for _, opt := range constraintOptions {
		opt(a)
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
