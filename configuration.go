package allocator

import "time"

const (
	noMaxChurn     = -1
	defaultTimeout = time.Second * 10
	loggingPrefix  = ""
	defaultRf      = 3
)

// Configuration holds runtime allocation preferences.
type Configuration struct {
	// withResources signals the allocator to perform balancing and capacity checking.
	withResources bool
	// withTagAffinity forces the allocator to perform affine allocations only.
	withTagAffinity bool
	// withMinimalChurn asks the allocator to reduce variance from a prior allocation.
	withMinimalChurn bool
	// maxChurn limits the number of moves needed to fulfill an allocation request with respect to a prior allocation.
	maxChurn int64
	// searchTimeout forces the solver to return within the specified duration.
	searchTimeout time.Duration
	// verboseLogging routes all the internal solver logs to stdout.
	verboseLogging bool
	// rf specifies the replication factor applied to all shards.
	rf int
}

// ConfigurationOption manifests a closure that mutates allocation configurations in accordance with caller preferences.
type ConfigurationOption func(*Configuration)

func NewConfiguration(opts ...ConfigurationOption) *Configuration {
	defaultConfiguration := Configuration{
		// assume no maxChurn initially, let the opts slice override if needed.
		maxChurn:      noMaxChurn,
		searchTimeout: defaultTimeout,
		rf:            defaultRf,
	}
	for _, opt := range opts {
		opt(&defaultConfiguration)
	}
	return &defaultConfiguration
}

func (c *Configuration) UpdateConfiguration(opts ...ConfigurationOption) {
	for _, opt := range opts {
		opt(c)
	}
}

// WithResources is a closure that configures the allocator to adhere to capacity constraints and load-balance across
// resources.
func WithResources(enable bool) ConfigurationOption {
	return func(opt *Configuration) {
		opt.withResources = enable
	}
}

// WithTagMatching is a closure that configures the allocator to perform affine allocations only.
func WithTagMatching(enable bool) ConfigurationOption {
	return func(opt *Configuration) {
		opt.withTagAffinity = enable
	}
}

// DisableMaxChurn an idempotent method to disable max churn boundary
func DisableMaxChurn() ConfigurationOption {
	return func(opt *Configuration) {
		opt.maxChurn = -1
	}
}

// WithMaxChurn is a closure that inspects and sets a hard limit on the maximum number of moves deviating
// from some prior assignment.
func WithMaxChurn(maxChurn int64) ConfigurationOption {
	return func(opt *Configuration) {
		if maxChurn < 0 {
			panic("max-churn must be greater than or equal to 0")
		}
		opt.maxChurn = maxChurn
	}
}

// WithChurnMinimized is a closure that configures the allocator to minimize variance from some prior allocation.
func WithChurnMinimized(enable bool) ConfigurationOption {
	return func(opt *Configuration) {
		opt.withMinimalChurn = enable
	}
}

// WithTimeout is a closure that configures the allocator to conclude its search within the duration provided.
func WithTimeout(searchTimeout time.Duration) ConfigurationOption {
	if searchTimeout < 0 {
		panic("searchTimeout cannot be negative")
	}
	return func(opt *Configuration) {
		opt.searchTimeout = searchTimeout
	}
}

// WithVerboseLogging is a closure that forces our solver to expose its logs to the caller for inspection.
func WithVerboseLogging(enable bool) ConfigurationOption {
	return func(opt *Configuration) {
		opt.verboseLogging = enable
	}
}

// WithReplicationFactor is a closure that overrides our default replication factor of 3.
func WithReplicationFactor(rf int) ConfigurationOption {
	return func(opt *Configuration) {
		if rf < 0 {
			panic("rf cannot be negative")
		}
		opt.rf = rf
	}
}
