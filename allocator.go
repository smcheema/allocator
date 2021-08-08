package allocator

import (
	"fmt"
	"github.com/irfansharif/or-tools/cpsatsolver"
)

type _range struct {
	tag int64
}

func allocate(ranges []_range) [][]int {
	model := cpsatsolver.NewModel()
	assignment := initAssignmentMatrix(model, ranges)
	addAssignToSomeNodeConstraint(model, assignment)
	return constructResult(model, assignment)
}

func initAssignmentMatrix(model *cpsatsolver.Model, ranges []_range) [][]cpsatsolver.Literal {
	assignment := make([][]cpsatsolver.Literal, len(ranges))
	for _rangeIndex := range assignment {
		assignment[_rangeIndex] = make([]cpsatsolver.Literal, ClusterSize)
		for nodeIndex := 0; nodeIndex < len(assignment[_rangeIndex]); nodeIndex++ {
			// let each entry in the assignment matrix be a boolean literal, indicating presence of a
			// replica on a specified node or the inverse.
			assignment[_rangeIndex][nodeIndex] = model.NewLiteral(fmt.Sprintf("Assignment for range %d to node %d", _rangeIndex, nodeIndex))
		}
	}
	return assignment
}

func addAssignToSomeNodeConstraint(model *cpsatsolver.Model, assignment [][]cpsatsolver.Literal) {
	for _rangeIndex := 0; _rangeIndex < len(assignment); _rangeIndex++ {
		model.AddConstraints(cpsatsolver.NewExactlyKConstraint(ReplicationFactor, assignment[_rangeIndex]...))
	}
}

func constructResult(model *cpsatsolver.Model, assignment [][]cpsatsolver.Literal) [][]int {
	result := model.Solve()
	allocatedAssignments := make([][]int, len(assignment))
	for index := range allocatedAssignments {
		allocatedAssignments[index] = make([]int, ClusterSize)
		for _assignmentIndex := range allocatedAssignments[index] {
			allocatedAssignments[index][_assignmentIndex] = int(result.Value(assignment[index][_assignmentIndex]))
		}
	}
	return allocatedAssignments
}
