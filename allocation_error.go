package allocator

import "fmt"

// RfGreaterThanClusterSizeError signifies an allocation error due to an insufficient node count for allocating replicas onto different nodes.
type RfGreaterThanClusterSizeError int64

// InsufficientClusterCapacityError indicates that the sum of range demands exceeds the sum of node capacities.
type InsufficientClusterCapacityError struct{}

// RangeWithWaywardTagsError indicates that there exists at-least one range whose tags are not held by any node.
type RangeWithWaywardTagsError int64

// InvalidModelError indicates that the model built is conflicting and not possible to solve.
type InvalidModelError struct{}

// CouldNotSolveError indicates that the solver could not reach an answer, either due to constraints that are too tight or a time-out.
type CouldNotSolveError struct{}

func (e RfGreaterThanClusterSizeError) Error() string {
	return fmt.Sprintf("rf passed in for rangeId:%d greater than cluster size", e)
}

func (e InsufficientClusterCapacityError) Error() string {
	return "sum of range demands exceed sum of node resources available"
}

func (e RangeWithWaywardTagsError) Error() string {
	return fmt.Sprintf("range with rangeId:%d holds tags that are not present on any node", e)
}

func (e InvalidModelError) Error() string {
	return "invalid model generated, inspect logs for further insights"
}

func (e CouldNotSolveError) Error() string {
	return "allocation failed, most likely due to a timeout or an infeasible model, use VerboseLogging to debug"
}
