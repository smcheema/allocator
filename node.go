package allocator

// nodeId is a 64-bit identifier for nodes.
type nodeId int64

// node encapsulates pertaining metadata for cockroachDB nodes.
type node struct {
	// id represents a unique identifier.
	id nodeId
	// tags are strings that showcase affinity for replicas.
	tags []string
	// resources model the Resource profile of said node.
	resources map[Resource]int64
}

func newNode(id nodeId) *node {
	return &node{
		id:        id,
		resources: make(map[Resource]int64),
	}
}

type NodeOption func(*node)

// WithTagsOfNode replaces tags of a node
func WithTagsOfNode(tags ...string) NodeOption {
	return func(modifiedNode *node) {
		modifiedNode.tags = tags
	}
}

// AddTagsToNode add tags to a node
// Note it does not check for uniqueness, perhaps we should change tags to a unique set instead of a slice
func AddTagsToNode(tags ...string) NodeOption {
	return func(modifiedNode *node) {
		modifiedNode.tags = append(modifiedNode.tags, tags...)
	}
}

// RemoveAllTagsOfNode will remove all tags of a node
func RemoveAllTagsOfNode() NodeOption {
	return func(modifiedNode *node) {
		modifiedNode.tags = nil
	}
}

// WithResourceOfNode will add or overwrite the amount of a target Resource the node provides
func WithResourceOfNode(targetResource Resource, resourceAmount int64) NodeOption {
	return func(modifiedNode *node) {
		modifiedNode.resources[targetResource] = resourceAmount
	}
}