package allocator

// nodeId is a 64-bit identifier for nodes.
type nodeId int64

// node encapsulates pertaining metadata for cockroachDB nodes.
type node struct {
	// id represents a unique identifier.
	id nodeId
	// tags are strings that showcase affinity for shards.
	// note: we key the following map using the tag
	// and assign an empty struct as the corresponding value
	// since we only care about tag membership
	// and not values assigned to said tag per se.
	tags map[string]struct{}
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
		tagsM := make(map[string]struct{})
		for _, tag := range tags {
			tagsM[tag] = struct{}{}
		}
		modifiedNode.tags = tagsM
	}
}

// AddTagsToNode add tags to a node
func AddTagsToNode(tags ...string) NodeOption {
	return func(modifiedNode *node) {
		for _, tag := range tags {
			// build empty struct, really a placeholder value to satisfy map semantics
			modifiedNode.tags[tag] = struct{}{}
		}
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
