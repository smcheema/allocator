package allocator

import "github.com/irfansharif/or-tools/cpsatsolver"

const (
	SizeOnDiskDemand = "SizeOnDisk"
)

const (
	DiskCapacityResource = "DiskSize"
)

type Resource string
type ResourceAmount int64
type Tag string
type TagValues []string
type NodeId int64

type Tags map[Tag]TagValues
type Resources map[Resource]ResourceAmount
type Cluster []node

type RangeId int64
type Demands map[Resource]ResourceAmount
type Assignments map[RangeId][]NodeId
type ConstraintMatrix [][]cpsatsolver.Literal

type node struct {
	nodeId    NodeId
	tags      Tags
	resources Resources
}

type Configuration struct {
	replicationFactor int
	nodes             Cluster
}

func initConfiguration(nodes Cluster, replicationFactor int) *Configuration {
	return &Configuration{
		replicationFactor: replicationFactor,
		nodes:             nodes,
	}
}

func (configuration *Configuration) getClusterSize() int {
	return len(configuration.nodes)
}

func (configuration *Configuration) getReplicationFactor() int {
	return configuration.replicationFactor
}
