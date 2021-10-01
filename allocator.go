package allocator

import (
	"fmt"
	"github.com/irfansharif/solver"
	"math"
	"sort"
)

type NodeID int64
type RangeID int64
type Resource int
type Allocation map[RangeID][]NodeID

const (
	DiskResource Resource = iota
	Qps
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
	sort.Slice(nodes,
		func(i, j int) bool {
			return nodes[i].id < nodes[j].id
		},
	)
	nIdDomain := make([]int64, 0)

	startIndex, endIndex := 0, 1

	for endIndex < len(nodes) {
		if nodes[endIndex].id != nodes[endIndex - 1].id + 1 {
			nIdDomain = append(nIdDomain, int64(nodes[startIndex].id), int64(nodes[endIndex - 1].id))
			startIndex = endIndex
		}
		endIndex++
	}
	if len(nodes) > 0 {
		nIdDomain = append(nIdDomain, int64(nodes[startIndex].id), int64(nodes[endIndex-1].id))
	}

	for _, r := range ranges {
		assignment[r.id] = make([]solver.IntVar, r.rf)
		for j := range assignment[r.id] {
			assignment[r.id][j] = model.NewIntVarFromDomain(
				solver.NewDomain(nIdDomain[0], nIdDomain[1], nIdDomain[2:]...),
				fmt.Sprintf("Allocation var for r.id:%d.", r.id))
		}
	}

	defaultOptions := options{}
	for _, opt := range opts {
		opt(&defaultOptions)
	}

	idToRange := make(map[RangeID]Range)
	for _, r := range ranges {
		idToRange[r.id] = r
	}

	idToNode := make(map[NodeID]Node)
	for _, n := range nodes {
		idToNode[n.id] = n
	}

	return &Allocator{
		ranges:     idToRange,
		nodes:      idToNode,
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

func (a *Allocator) PrintModel() {
	fmt.Println(a.model.String())
}

func (a *Allocator) intVar(rid RangeID) []solver.IntVar {
	return a.assignment[rid]
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
		for _, n := range a.nodes {
			if c, found := n.resources[re]; !found {
				maxCapacity = math.MaxInt32
				break
			} else if c > maxCapacity {
				maxCapacity = c
			}
		}

		tasks := make([]solver.Interval, 0)
		demands := make([]solver.IntVar, 0)

		// for each node n, compute offset from maxCapacity, and add an interval with dimensions [nodeID, nodeID +1)
		// holding magnitude maxCapacity - n.resources[re], this ensures that only n.resources[re] worth of capacity remains.
		fixedSizedOneOffset := a.model.NewConstant(1, fmt.Sprintf("Fixed offset of size 1."))
		for _, node := range a.nodes {
			if c, found := node.resources[re]; found {
				toAdd := a.model.NewInterval(
					a.model.NewConstant(int64(node.id), fmt.Sprintf("Fixed offset LB for n.id:%d.", node.id)),
					a.model.NewIntVar(1, 200, fmt.Sprintf("Fixed offset UB for n.id:%d.", node.id)),
					fixedSizedOneOffset,
					fmt.Sprintf("Fixed offset var for n.id:%d.", node.id),
				)
				tasks = append(tasks, toAdd)
				demands = append(demands, a.model.NewConstant(maxCapacity - c, fmt.Sprintf("Fixed offset magnitude for n.id:%d.", node.id)))
			}
		}

		// build demands for each node.
		for rangeID, nodeIDs := range a.assignment {
			for _, id := range nodeIDs {
				toAdd := a.model.NewInterval(
					id,
					a.model.NewIntVar(1, 200, "Placeholder upper bound for node-IntVar."),
					fixedSizedOneOffset,
					"Interval representing demands placed on a node.",
				)
				tasks = append(tasks, toAdd)
				demands = append(demands, a.model.NewConstant(a.ranges[rangeID].demands[re], fmt.Sprintf("Demand for r.id:%d.", rangeID)))
			}
		}

		if re == 0 {
			a.model.AddConstraints(
				solver.NewCumulativeConstraint(a.model.NewConstant(maxCapacity, "maxCapacity"),
					tasks, demands,
				),
			)
		}

		pushDown := a.model.NewIntVar(0, math.MaxInt32, fmt.Sprintf("IntVar that pushes down for resource: %d", re))
		a.model.AddConstraints(
			solver.NewCumulativeConstraint(pushDown,
				tasks, demands,
			),
		)
		a.model.Minimize(solver.Sum(pushDown))
	}
}

func (a *Allocator) adhereToNodeTags() {
	for i, r := range a.ranges {
		forbiddenAssignments := make([][]int64, 0)
		for j, n := range a.nodes {
			if !rangeTagsAreSubsetOfNodeTags(r.tags, n.tags) {
				forbiddenAssignments = append(forbiddenAssignments, []int64{int64(j)})
			}
		}
		a.model.AddConstraints(solver.NewForbiddenAssignmentsConstraint(
			a.intVar(i), forbiddenAssignments,
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

func (a *Allocator) Allocate() (ok bool, allocation Allocation) {
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

	fmt.Println(a.model.String())

	result := a.model.Solve()
	if result.Infeasible() || result.Invalid() {
		return false, nil
	}

	res := make(Allocation)
	for i, r := range a.ranges {
		nodes := a.intVar(i)
		for _, n := range nodes {
			allocated := result.Value(n)
			res[r.id] = append(res[r.id], NodeID(allocated))
		}
	}
	return true, res
}