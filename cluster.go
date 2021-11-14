package allocator

// Resource is 32-bit identifier for resources -> {disk, QPS}
type Resource int

const (
	DiskResource Resource = iota
	QPS
)

type ClusterState struct {
	nodes             map[nodeId]*node
	replicas          map[replicaId]*replica
	currentAssignment map[replicaId][]nodeId
}

func NewClusterState() *ClusterState {
	return &ClusterState{
		nodes:             make(map[nodeId]*node),
		replicas:          make(map[replicaId]*replica),
		currentAssignment: make(map[replicaId][]nodeId),
	}
}

// AddReplica will add or overwrite the existing replica given an ID
func (cs *ClusterState) AddReplica(id int64, rf int, opts ...ReplicaOption) {
	if id < 0 {
		panic("range id cannot be negative")
	} else if rf < 0 {
		panic("range rf cannot be negative")
	}
	newReplica := newReplica(replicaId(id), rf)
	for _, opt := range opts {
		opt(newReplica)
	}
	cs.replicas[replicaId(id)] = newReplica
}

// RemoveReplica removes a given replica from the cluster
// no-op if it doesn't exist.
func (cs *ClusterState) RemoveReplica(id int64) {
	delete(cs.replicas, replicaId(id))
}

// UpdateReplica will update the existing replica given an ID
// returning true for a successful update, false if the replicaID does not map to any replica.
func (cs *ClusterState) UpdateReplica(id int64, opts ...ReplicaOption) bool {
	if modifiedReplicaPtr, found := cs.replicas[replicaId(id)]; found {
		for _, opt := range opts {
			opt(modifiedReplicaPtr)
		}
		cs.replicas[replicaId(id)] = modifiedReplicaPtr

		return true
	}
	return false
}

// AddNode will add or overwrite the existing node given an ID
func (cs *ClusterState) AddNode(id int64, opts ...NodeOption) {
	if id < 0 {
		panic("node id cannot be negative")
	}
	newNode := newNode(nodeId(id))
	for _, opt := range opts {
		opt(newNode)
	}
	cs.nodes[nodeId(id)] = newNode
}

// RemoveNode removes a given node from the cluster
// no-op if it doesn't exist.
func (cs *ClusterState) RemoveNode(id int64) {
	delete(cs.nodes, nodeId(id))
}

// UpdateNode will update the existing node given an ID
// returning true for a successful update, false if the nodeID does not map to any node.
func (cs *ClusterState) UpdateNode(id int64, opts ...NodeOption) bool {
	if modifiedNodePtr, found := cs.nodes[nodeId(id)]; found {
		for _, opt := range opts {
			opt(modifiedNodePtr)
		}
		cs.nodes[nodeId(id)] = modifiedNodePtr

		return true
	}
	return false
}

// UpdateCurrentAssignment store the current allocation state to potentially guide the solver
func (cs *ClusterState) UpdateCurrentAssignment(priorAllocation Allocation) {
	temp := make(map[replicaId][]nodeId)
	for r, n := range priorAllocation {
		temp[replicaId(r)] = make([]nodeId, len(n))
		for i, nid := range n {
			temp[replicaId(r)][i] = nodeId(nid)
		}
	}
	cs.currentAssignment = temp
}
