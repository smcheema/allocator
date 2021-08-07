package allocator

import (
	"fmt"
	"github.com/irfansharif/or-tools/cpsatsolver"
)

type node struct {
	tag int64
}

type _range struct {
	tag int64
}

const ClusterSize = 64

func allocate(ranges []_range) [][]int {
	// basic allocator, given ranges, assign
	// all ranges to some node.
	model := cpsatsolver.NewModel()
	// assignment matrix, [i][j] == 1 means
	// range i was deployed onto node j
	assignment := make([][]cpsatsolver.IntVar, len(ranges))
	// this is our return matrix, init'zed here for visibility
	// and separation from or-tools logic.
	allocatedAssignments := make([][]int, len(ranges))
	for index := range assignment {
		assignment[index] = make([]cpsatsolver.IntVar, ClusterSize)
	}
	for index := range allocatedAssignments {
		allocatedAssignments[index] = make([]int, ClusterSize)
	}

	addAssignToSomeNodeConstraint(model, assignment)
	result := model.Solve()
	for _rangeIndex, _range := range assignment {
		for _assignmentIndex := range _range {
			allocatedAssignments[_rangeIndex][_assignmentIndex] = int(result.Value(assignment[_rangeIndex][_assignmentIndex]))
		}
	}
	return allocatedAssignments
}

func addAssignToSomeNodeConstraint(model *cpsatsolver.Model, assignment [][]cpsatsolver.IntVar) {
	// used to build the linearExpression that we need to constrain
	// each range being assigned to one and only one node.
	coefficients := make([]int64, len(assignment[0]))
	for index := range coefficients {
		coefficients[index] = 1
	}
	for _rangeIndex := 0; _rangeIndex < len(assignment); _rangeIndex++ {
		for nodeIndex := 0; nodeIndex < len(assignment[_rangeIndex]); nodeIndex++ {
			// let each entry in the assignment matrix be a boolean literal, indicating presence of a
			// range on a specified node or the inverse.
			assignment[_rangeIndex][nodeIndex] = model.NewIntVar(0, 1, fmt.Sprintf("Assignment for range %d to node %d", _rangeIndex, nodeIndex))
		}
		// constraint each range to exist on exactly one node.
		model.AddConstraints(
			cpsatsolver.NewLinearConstraint(cpsatsolver.NewLinearExpr(assignment[_rangeIndex], coefficients, 0),
			cpsatsolver.NewDomain(1, 1)))
	}
}
