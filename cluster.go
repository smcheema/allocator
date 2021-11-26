package allocator

// Resource is 32-bit identifier for resources -> {disk, QPS}
type Resource int

const (
	DiskResource Resource = iota
	QPS
)

type ClusterState struct {
	nodes             map[nodeId]*node
	shards            map[shardId]*shard
	currentAssignment map[shardId][]nodeId
}

func NewClusterState() *ClusterState {
	return &ClusterState{
		nodes:             make(map[nodeId]*node),
		shards:            make(map[shardId]*shard),
		currentAssignment: make(map[shardId][]nodeId),
	}
}

// AddShard will add or overwrite the existing shard given a shardId
func (cs *ClusterState) AddShard(id int64, rf int, opts ...ShardOption) {
	if id < 0 {
		panic("range id cannot be negative")
	} else if rf < 0 {
		panic("range rf cannot be negative")
	}
	s := newShard(shardId(id), rf)
	for _, opt := range opts {
		opt(s)
	}
	cs.shards[shardId(id)] = s
}

// RemoveShard removes a given shard from the cluster
// no-op if it doesn't exist.
func (cs *ClusterState) RemoveShard(id int64) {
	delete(cs.shards, shardId(id))
}

// UpdateShard will update the existing shard given a shardId
// returning true for a successful update, false if the shardId does not map to any shard.
func (cs *ClusterState) UpdateShard(id int64, opts ...ShardOption) bool {
	if s, found := cs.shards[shardId(id)]; found {
		for _, opt := range opts {
			opt(s)
		}
		cs.shards[shardId(id)] = s
		return true
	}
	return false
}

// AddNode will add or overwrite the existing node given a nodeId
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

// UpdateNode will update the existing node given a nodeId
// returning true for a successful update, false if the nodeId does not map to any node.
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
	temp := make(map[shardId][]nodeId)
	for r, n := range priorAllocation {
		temp[shardId(r)] = make([]nodeId, len(n))
		for i, nid := range n {
			temp[shardId(r)][i] = nodeId(nid)
		}
	}
	cs.currentAssignment = temp
}
