package allocator

import (
	"fmt"
	"github.com/irfansharif/solver"
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
	model      *solver.Model
	// map[RangeId]map[NodeId]Literal
	// n x r variables
	// 1000, 10k, 10^8 variables
	// dependency on nodes --> having it rely soley on ranges
	// index into the range slice, maps to an IntVar
	// where does this range end up? Which node do I put this range onto.
	// sum of all replication factors
	assignment map[int][]solver.IntVar
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
	model := solver.NewModel("")
	assignment := make(map[int][]solver.IntVar)
	for i, r := range ranges {
		assignment[i] = make([]solver.IntVar, r.rf)
		for j := range assignment[i] {
			assignment[i][j] = model.NewIntVar(0, int64(len(nodes)-1), "")
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

func (a *Allocator) intVar(i int) []solver.IntVar {
	return a.assignment[i]
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
		maxCapacity := int64(0)
		// compute maxCap --> the highest capacity available inside nodeset
		// [100, 200, 300] --> maxCap = 300
		for _, n := range a.nodes {
			if n.resources[re] > maxCapacity {
				maxCapacity = n.resources[re]
			}
		}

		tasks := make([]solver.Interval, 0)
		demands := make([]solver.IntVar, 0)
		sumOfDemands := int64(0)
		// [whereDoesThisRangeGo(index into the nodes array), whereDoesThisRangeGo(index into the nodes array) + 1, 1)
		// specified demand --> thisRange's demand --> how much resource does thisRange need.
		// rangeDemands : [10, 5, 20] --> assignment[0] = whereDoesR0Go, whereDoesR1Go, whereDoesR2Go
		// nodesDemands : [15, 20, 25]
		for rangeIndex, nodeIndexes := range a.assignment {
			for _, nodeIndex := range nodeIndexes {
				// [whereDoesR0Go, whereDoesR0Go + 1) --> point on the x - axis
				// R0's demand
				// R0 demand of 15
				toAdd := a.model.NewInterval(
					nodeIndex, a.model.NewIntVar(1, int64(len(a.nodes)), ""),
					a.model.NewConstant(1,  ""), "",
				)
				tasks = append(
					tasks, toAdd,
				)
				demands = append(demands, a.model.NewConstant(a.ranges[rangeIndex].demands[re], ""))
				sumOfDemands += a.ranges[rangeIndex].demands[re]
			}
		}

		for i, n := range a.nodes {
			// rangeDemands : [10, 5, 20] --> assignment[0] = whereDoesR0Go, whereDoesR1Go, whereDoesR2Go
			// nodesDemands : [15, 20, 25]
			// R0 --> maxCapacity - 15
			toAdd := a.model.NewInterval(
				a.model.NewIntVar(int64(i), int64(i), ""), a.model.NewIntVar(1, int64(len(a.nodes)), ""),
				a.model.NewIntVar(1, 1, ""), "",
			)
			tasks = append(
				tasks, toAdd,
			)
			demands = append(demands, a.model.NewConstant(maxCapacity - n.resources[re], ""))
			sumOfDemands += maxCapacity - n.resources[re]
		}
		a.model.AddConstraints(
			solver.NewCumulativeConstraint(a.model.NewConstant(maxCapacity, ""),
				tasks, demands,
			),
		)
	}
}

func (a *Allocator) adhereToNodeTags() {
	for i, r := range a.ranges {
		for j, n := range a.nodes {
			if !rangeTagsAreSubsetOfNodeTags(r.tags, n.tags) {
				a.model.AddConstraints(solver.NewForbiddenAssignmentsConstraint(
						a.intVar(i), [][]int64{{int64(j)}},
				))
			}
		}
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

	for _, r := range a.assignment {
		a.model.AddConstraints(solver.NewAllDifferentConstraint(r...))
	}

	ok, err := a.model.Validate()
	if !ok {
		fmt.Println(err)
	}

	result := a.model.Solve()
	if result.Infeasible() || result.Invalid() {
		return false, nil
	}

	res := make(map[RangeID][]NodeID)
	for i, r := range a.ranges {
		nodes := a.intVar(i)
		for _, n := range nodes {
			allocated := result.Value(n)
			res[r.id] = append(res[r.id], a.nodes[allocated].id)
		}
	}
	return true, res
}