package allocator

// replicaId is a 64-bit identifier for replicas.
type replicaId int64

// replica encapsulates pertaining metadata for cockroachDB replicas.
type replica struct {
	// id represents a unique identifier.
	id replicaId
	// rf equals the replication factor of said replica.
	rf int
	// tags are strings that showcase affinity for replicas.
	// note: we key the following map using the tag
	// and assign an empty struct as the corresponding value
	// since we only care about tag membership
	// and not values assigned to said tag per se.
	tags map[string]struct{}
	// demands model the Resource requirements of said replica.
	demands map[Resource]int64
}

func newReplica(id replicaId, rf int) *replica {
	return &replica{
		id:      id,
		rf:      rf,
		demands: make(map[Resource]int64),
	}
}

type ReplicaOption func(*replica)

// WithReplicationFactorOfReplica updates replication factor of a replica
func WithReplicationFactorOfReplica(replicationFactor int) ReplicaOption {
	return func(modifiedReplica *replica) {
		modifiedReplica.rf = replicationFactor
	}
}

// WithTagsOfReplica replaces tags of a replica
func WithTagsOfReplica(tags ...string) ReplicaOption {
	return func(modifiedReplica *replica) {
		tagsM := make(map[string]struct{})
		for _, tag := range tags {
			tagsM[tag] = struct{}{}
		}
		modifiedReplica.tags = tagsM
	}
}

// AddTagsToReplica add tags to a replica
func AddTagsToReplica(tags ...string) ReplicaOption {
	return func(modifiedReplica *replica) {
		for _, tag := range tags {
			// build empty struct, really a placeholder value to satisfy map semantics
			modifiedReplica.tags[tag] = struct{}{}
		}
	}
}

// RemoveAllTagsOfReplica will remove all tags of a replica
func RemoveAllTagsOfReplica() ReplicaOption {
	return func(modifiedReplica *replica) {
		modifiedReplica.tags = nil
	}
}

// WithDemandOfReplica will add or overwrite the amount of a target Resource the replica demands
func WithDemandOfReplica(targetResource Resource, demandAmount int64) ReplicaOption {
	if demandAmount < 0 {
		panic("resourceAmount cannot be negative")
	}
	return func(modifiedReplica *replica) {
		modifiedReplica.demands[targetResource] = demandAmount
	}
}
