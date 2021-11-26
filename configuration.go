package allocator

import "time"

const (
	noMaxChurn     = -1
	defaultTimeout = time.Second * 10
	loggingPrefix  = ""
)

// configuration holds runtime allocation preferences.
type configuration struct {
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
}

// Option manifests a closure that mutates allocation configurations in accordance with caller preferences.
type Option func(*configuration)

// WithResources is a closure that configures the allocator to adhere to capacity constraints and load-balance across
// resources.
func WithResources() Option {
	return func(opt *configuration) {
		opt.withResources = true
	}
}

// WithTagMatching is a closure that configures the allocator to perform affine allocations only.
func WithTagMatching() Option {
	return func(opt *configuration) {
		opt.withTagAffinity = true
	}
}

// WithMaxChurn is a closure that inspects and sets a hard limit on the maximum number of moves deviating
// from some prior assignment.
func WithMaxChurn(maxChurn int64) Option {
	return func(opt *configuration) {
		if maxChurn < 0 {
			panic("max-churn must be greater than or equal to 0")
		}
		opt.maxChurn = maxChurn
	}
}

// WithChurnMinimized is a closure that configures the allocator to minimize variance from some prior allocation.
func WithChurnMinimized() Option {
	return func(opt *configuration) {
		opt.withMinimalChurn = true
	}
}

// WithTimeout is a closure that configures the allocator to conclude its search within the duration provided.
func WithTimeout(searchTimeout time.Duration) Option {
	if searchTimeout < 0 {
		panic("searchTimeout cannot be negative")
	}
	return func(opt *configuration) {
		opt.searchTimeout = searchTimeout
	}
}

// WithVerboseLogging is a closure that forces our solver to expose its logs to the caller for inspection.
func WithVerboseLogging() Option {
	return func(opt *configuration) {
		opt.verboseLogging = true
	}
}
