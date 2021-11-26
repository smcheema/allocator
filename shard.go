package allocator

// shardId is a 64-bit identifier for shards.
type shardId int64

// shard encapsulates pertaining metadata for cockroachDB shards.
type shard struct {
	// id represents a unique identifier.
	id shardId
	// rf equals the replication factor of said shard.
	rf int
	// tags are strings that showcase affinity for shards.
	// note: we key the following map using the tag
	// and assign an empty struct as the corresponding value
	// since we only care about tag membership
	// and not values assigned to said tag per se.
	tags map[string]struct{}
	// demands model the Resource requirements of said shard.
	demands map[Resource]int64
}

func newShard(id shardId, rf int) *shard {
	return &shard{
		id:      id,
		rf:      rf,
		demands: make(map[Resource]int64),
	}
}

type ShardOption func(*shard)

// WithReplicationFactor updates the replication factor of a shard
func WithReplicationFactor(replicationFactor int) ShardOption {
	return func(s *shard) {
		s.rf = replicationFactor
	}
}

// WithTagsOfShard replaces tags of a shard
func WithTagsOfShard(tags ...string) ShardOption {
	return func(s *shard) {
		tagsM := make(map[string]struct{})
		for _, tag := range tags {
			tagsM[tag] = struct{}{}
		}
		s.tags = tagsM
	}
}

// AddTagsToShard add tags to a shard
func AddTagsToShard(tags ...string) ShardOption {
	return func(s *shard) {
		for _, tag := range tags {
			// build empty struct, really a placeholder value to satisfy map semantics
			s.tags[tag] = struct{}{}
		}
	}
}

// RemoveAllTagsOfShard will remove all tags of a shard
func RemoveAllTagsOfShard() ShardOption {
	return func(s *shard) {
		s.tags = nil
	}
}

// WithDemandOfShard will add or overwrite the amount of a target Resource the shard demands
func WithDemandOfShard(targetResource Resource, demandAmount int64) ShardOption {
	return func(s *shard) {
		s.demands[targetResource] = demandAmount
	}
}
