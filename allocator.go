package allocator

import (
	"fmt"
	"github.com/irfansharif/solver"
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
	withResources   bool
	withTagAffinity bool
}

type Option func(*options)

type Allocator struct {
	ranges      map[RangeID]Range
	nodes       map[NodeID]Node
	model       *solver.Model
	assignment  map[RangeID][]solver.IntVar
	opts        options
	nodeDomains []int64
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

	// build disjoint intervals, these help keep our domains as tight as possible.
	// an example of where this computation helps would be being passed nodeIds : [0, 1, 1000]
	// this would then split the above into [0, 1] and [1, 1000].
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
		ranges:      idToRange,
		nodes:       idToNode,
		model:       model,
		assignment:  assignment,
		opts:        defaultOptions,
		nodeDomains: nIdDomain,
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
		opt.withResources = true
	}
}

func WithTagMatching() Option {
	return func(opt *options) {
		opt.withTagAffinity = true
	}
}

func (a *Allocator) adhereToNodeResources() {
	// build demands for each node.
	upperNodeIdDomain := append(make([]int64, 0, len(a.nodeDomains)), a.nodeDomains...)
	// our intervals are built as: [nodeId, nodeId + 1).
	// the below helps with building out what the domain would look like for the second argument above.
	for i := range upperNodeIdDomain {
		upperNodeIdDomain[i]++
	}
	// build this here to reduce our ever-expanding variable set.
	fixedSizedOneOffset := a.model.NewConstant(1, fmt.Sprintf("Fixed offset of size 1."))
	for _, re := range []Resource{DiskResource, Qps} {
		maxCapacity := int64(0)
		totalDemand := int64(0)
		// tasks pertain to node's here. Each task spans [nodeID, nodeID + 1)
		tasks := make([]solver.Interval, 0)
		// demands pertain to the asks placed on each node. These are primarily composed of
		// fixed offsets to even out node capacity + demands put forth by the ranges that will ultimately be assigned
		// to said nodes.
		demands := make([]solver.IntVar, 0)
		for _, r := range a.ranges {
			totalDemand += r.demands[re] * int64(r.rf)
		}
		// go over cluster, compute maxCapacity
		// if a node does not define a capacity for a specific resource,
		// assume it to equal +inf. We represent +inf as totalDemand (how much we're looking to allocate)
		// to keep our bounds as tight as possible.
		for _, n := range a.nodes {
			if c, found := n.resources[re]; !found {
				maxCapacity = totalDemand
				break
			} else if c > maxCapacity {
				maxCapacity = c
			}
		}

		for rangeID, nodeIDs := range a.assignment {
			for i, id := range nodeIDs {
				toAdd := a.model.NewInterval(
					id,
					a.model.NewIntVarFromDomain(solver.NewDomain(upperNodeIdDomain[0], upperNodeIdDomain[1], upperNodeIdDomain[2:]...), "Adjusted intervals for upper bounds."),
					fixedSizedOneOffset,
					fmt.Sprintf("Interval representing demands placed on node by range: %d, replica: %d", rangeID, i),
				)
				tasks = append(tasks, toAdd)
				demands = append(demands, a.model.NewConstant(a.ranges[rangeID].demands[re], fmt.Sprintf("Demand for r.id:%d.", rangeID)))
			}
		}

		// do this now, as opposed to doing it after adding our fixed offsets. This is because balancing becomes tricky
		// with fixed sized blocks. Consider nodes with some arbitrary capacity defined: [10, 50, 90],
		// and ranges with arbitrary demands defined: [10, 20, 30]. If we ask to balance after adding our fixed offsets,
		// we're asking to balance across node0 with fixed offset (80), node1 with fixed offset (40), and node2 across fixed offset 0.
		// This causes the allocator to imbalance all placement over to the last node.
		minimizeVariance := a.model.NewIntVar(0, totalDemand, fmt.Sprintf("IntVar that pushes down for resource: %d", re))
		a.model.AddConstraints(
			solver.NewCumulativeConstraint(minimizeVariance,
				tasks, demands,
			),
		)
		// this completely bottlenecks our performance. It completely blasts up over small increases to cluster sizes.
		// I have tried different ways of modelling constraints, inspecting model variables as a function of cluster size,
		// but no luck as yet. this doesn't seem straightforward, most likely a lot of playing around will either expose
		// an inefficiency in my design or something deeper.
		a.model.Minimize(solver.Sum(minimizeVariance))


		// for each node n, compute offset from maxCapacity, and add an interval with dimensions [nodeID, nodeID +1)
		// holding magnitude maxCapacity - n.resources[re], this ensures that only n.resources[re] worth of capacity remains.
		for _, node := range a.nodes {
			if c, found := node.resources[re]; found {
				toAdd := a.model.NewInterval(
					a.model.NewConstant(int64(node.id), fmt.Sprintf("Fixed offset LB for n.id:%d.", node.id)),
					a.model.NewConstant(int64(node.id) + 1, fmt.Sprintf("Fixed offset UB for n.id:%d.", node.id)),
					fixedSizedOneOffset,
					fmt.Sprintf("Fixed offset var for n.id:%d.", node.id),
				)
				tasks = append(tasks, toAdd)
				demands = append(demands, a.model.NewConstant(maxCapacity - c, fmt.Sprintf("Fixed offset magnitude for n.id:%d.", node.id)))
			}
		}

		// ensure capacity constraints are respected.
		capacityLimit := a.model.NewIntVar(0, maxCapacity, fmt.Sprintf("IntVar that dictates capacity constraints for resource: %d", re))
		a.model.AddConstraints(
			solver.NewCumulativeConstraint(capacityLimit,
				tasks, demands,
			),
		)
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
	if a.opts.withResources {
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