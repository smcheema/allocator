package allocator

import (
	"fmt"

	"github.com/irfansharif/or-tools/cpsatsolver"
)

type RangeID int64

type Range struct {
	id   RangeID
	rf   int
	tags []string

	demands map[Resource]int64
}

type NodeID int64

type Node struct {
	id   NodeID
	tags []string

	resources map[Resource]int64
}

type Allocator struct {
	ranges []Range
	nodes  []Node
	model  *cpsatsolver.Model

	assignment map[RangeID]map[NodeID]cpsatsolver.Literal
	previous   map[RangeID][]NodeID
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

func (a *Allocator) Allocate() (ok bool, assignments map[RangeID][]NodeID) {
	for _, r := range a.ranges {
		a.model.AddConstraints(cpsatsolver.NewExactlyKConstraint(r.rf, a.rangeLiterals(r)...))
	}

	for _, re := range []Resource{DiskResource, MemoryResource} {
		for _, n := range a.nodes {
			capacity := n.resources[re]

			var vars []cpsatsolver.IntVar
			var coeffs []int64
			for _, r := range a.ranges {
				vars = append(vars, a.literal(r, n))
				coeffs = append(coeffs, r.demands[re])
			}

			a.model.AddConstraints(cpsatsolver.NewLinearConstraint(
				cpsatsolver.NewLinearExpr(vars, coeffs, 0),
				cpsatsolver.NewDomain(0, capacity)))
		}
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
	a.previous = res
	return true, res
}

type Resource int

const (
	DiskResource Resource = iota
	MemoryResource
)
