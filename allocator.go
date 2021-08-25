package allocator

import (
	"fmt"
	"github.com/irfansharif/or-tools/cpsatsolver"
)

type _range struct {
	rangeId   RangeId
	tags 	  Tags
	demands   Demands
}

type Allocator interface {
	addAssignLikeReplicasToDifferentNodesConstraint()
	allocate() (ok bool, assignments Assignments)
}

type CKAllocator struct {
	ranges   []_range
	config   *Configuration
	model    *cpsatsolver.Model
	constraintMatrix ConstraintMatrix
}

func initAllocator(ranges []_range, config *Configuration) *CKAllocator {
	model := cpsatsolver.NewModel()
	constraintMatrix := initAssignmentMatrix(model, ranges, config.getClusterSize())
	return &CKAllocator {
		ranges: ranges,
		config: config,
		model: model,
		constraintMatrix: constraintMatrix,
	}
}

func (allocator *CKAllocator) addAssignLikeReplicasToDifferentNodesConstraint() {
	for _rangeIndex := 0; _rangeIndex < len(allocator.constraintMatrix); _rangeIndex++ {
		allocator.model.AddConstraints(cpsatsolver.NewExactlyKConstraint(allocator.config.getReplicationFactor(), allocator.constraintMatrix[_rangeIndex]...))
	}
}

func (allocator *CKAllocator) allocate() (ok bool, assignments Assignments) {
	return solveAndConstructResult(allocator.model, allocator.constraintMatrix, allocator.ranges)
}

func initAssignmentMatrix(model *cpsatsolver.Model, ranges []_range, clusterSize int) ConstraintMatrix {
	assignment := make(ConstraintMatrix, len(ranges))
	for _rangeIndex := range assignment {
		assignment[_rangeIndex] = make([]cpsatsolver.Literal, clusterSize)
		for nodeIndex := 0; nodeIndex < len(assignment[_rangeIndex]); nodeIndex++ {
			assignment[_rangeIndex][nodeIndex] = model.NewLiteral(fmt.Sprintf("Assignment for range %d to node %d", _rangeIndex, nodeIndex))
		}
	}
	return assignment
}

func solveAndConstructResult(model *cpsatsolver.Model, assignment ConstraintMatrix, ranges []_range) (bool, Assignments) {
	result := model.Solve()
	if result.Infeasible() || result.Invalid() {
		return false, nil
	} else {
		allocatedAssignments := make(Assignments)
		for index := range assignment {
			for _assignmentIndex := range assignment[index] {
				tempCompute := result.Value(assignment[index][_assignmentIndex])
				if tempCompute > 0 {
					allocatedAssignments[ranges[index].rangeId] = append(allocatedAssignments[ranges[index].rangeId], NodeId(_assignmentIndex))
				}
			}
		}
		return true, allocatedAssignments
	}
}
