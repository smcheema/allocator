package allocator

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// bazel test ... --test_output=all   --cache_test_results=no   --test_arg='-test.v'   --test_filter='Test.*' --test_env=TEST_TMPDIR='/home/azerila/dev/cap/temp/'
var dataOutputRoot = filepath.Join("/", "Users", "saadmusani", "Documents", "GitHub", "allocator1","json")

// configurationJson unexported version of Configuration. Exports the attributes for json to serialize
type configurationJson struct {
	// WithCapacity signals the allocator to perform capacity checking.
	WithCapacity bool
	// WithLoadBalancing signals the allocator to perform load balancing.
	WithLoadBalancing bool
	// WithTagAffinity forces the allocator to perform affine allocations only.
	WithTagAffinity bool
	// WithMinimalChurn asks the allocator to reduce variance from a prior allocation.
	WithMinimalChurn bool
	// MaxChurn limits the number of moves needed to fulfill an allocation request with respect to a prior allocation.
	MaxChurn int64
	// SearchTimeout forces the solver to return within the specified duration.
	SearchTimeout time.Duration
	// VerboseLogging routes all the internal solver logs to stdout.
	VerboseLogging bool
	// Rf specifies the replication factor applied to all shards.
	Rf int
}

func (c Configuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		configurationJson{
			WithCapacity:      c.withCapacity,
			WithLoadBalancing: c.withLoadBalancing,
			WithTagAffinity:   c.withTagAffinity,
			WithMinimalChurn:  c.withMinimalChurn,
			MaxChurn:          c.maxChurn,
			SearchTimeout:     c.searchTimeout,
			VerboseLogging:    c.verboseLogging,
			Rf:                c.rf,
		},
	)
}

// nodeJson unexported version of node. Exports the attributes for json to serialize
type nodeJson struct {
	// id represents a unique identifier.
	Id nodeId
	// tags are strings that showcase affinity for shards.
	// note: we key the following map using the tag
	// and assign an empty struct as the corresponding value
	// since we only care about tag membership
	// and not values assigned to said tag per se.
	Tags map[string]struct{}
	// resources model the Resource profile of said node.
	Resources map[Resource]int64
}

func (n node) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		nodeJson{
			Id:        n.id,
			Tags:      n.tags,
			Resources: n.resources,
		},
	)
}

// shardJson unexported version of shard. Exports the attributes for json to serialize
type shardJson struct {
	// id represents a unique identifier.
	Id shardId
	// tags are strings that showcase affinity for shards.
	// note: we key the following map using the tag
	// and assign an empty struct as the corresponding value
	// since we only care about tag membership
	// and not values assigned to said tag per se.
	Tags map[string]struct{}
	// demands model the Resource requirements of said shard.
	Demands map[Resource]int64
}

func (s shard) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		shardJson{
			Id:      s.id,
			Tags:    s.tags,
			Demands: s.demands,
		},
	)
}

// clusterStateJson unexported version of clusterState. Exports the attributes for json to serialize
type clusterStateJson struct {
	Nodes             map[nodeId]*node
	Shards            map[shardId]*shard
	CurrentAssignment map[shardId][]nodeId
}

func (cs ClusterState) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		clusterStateJson{
			Nodes:             cs.nodes,
			Shards:            cs.shards,
			CurrentAssignment: cs.currentAssignment,
		},
	)
}

type stepJson struct {
	ClusterState  *ClusterState
	Configuration *Configuration
	TimeInMs      int
	T             int
}

type Serializer struct {
	path string
}

func NewSerializer(testName string) (s Serializer) {
	t := time.Now()
	nowStr := t.Format("2006-01-02_15.04.05")
	path := filepath.Join(dataOutputRoot, testName, nowStr)
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		log.Println("damn")
		panic(err)
	}

	return Serializer{
		path: path,
	}
}

func allocationToAssignment(allocation Allocation) map[shardId][]nodeId {
	res := make(map[shardId][]nodeId)
	for k, v := range allocation {
		for _, nId := range v {
			res[shardId(k)] = append(res[shardId(k)], nodeId(nId))
		}
	}
	return res
}

func (s Serializer) WriteToFile(t int, clusterState *ClusterState, configuration *Configuration, newAllocation Allocation, timeInMs int) {

	clusterState.currentAssignment = allocationToAssignment(newAllocation)
	step := stepJson{
		ClusterState:  clusterState,
		Configuration: configuration,
		TimeInMs:      timeInMs,
		T:             t,
	}
	byteArray, err := json.Marshal(step)
	if err != nil {
		panic(err)
	}

	// open output file
	fo, err := os.Create(filepath.Join(s.path, strconv.Itoa(t)+".json"))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()

	if _, err := fo.Write(byteArray); err != nil {
		panic(err)
	}
}
