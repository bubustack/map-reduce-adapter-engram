package config

import (
	"time"

	"github.com/bubustack/bobrapet/pkg/refs"
)

// Config defines static defaults for the map-reduce adapter.
type Config struct {
	DefaultBatchSize                int           `json:"defaultBatchSize,omitempty"`
	DefaultConcurrency              int           `json:"defaultConcurrency,omitempty"`
	MaxBatchSize                    int           `json:"maxBatchSize,omitempty"`
	MaxConcurrency                  int           `json:"maxConcurrency,omitempty"`
	DefaultFailFast                 *bool         `json:"defaultFailFast,omitempty"`
	DefaultAllowFailures            *bool         `json:"defaultAllowFailures,omitempty"`
	DefaultInlineResultsLimit       int           `json:"defaultInlineResultsLimit,omitempty"`
	PollInterval                    time.Duration `json:"pollInterval,omitempty"`
	DefaultStoryRunTTLSeconds       *int32        `json:"defaultStoryRunTTLSeconds,omitempty"`
	DefaultStoryRunRetentionSeconds *int32        `json:"defaultStoryRunRetentionSeconds,omitempty"`
}

// Inputs represents the request payload for the adapter.
type Inputs struct {
	Items   any            `json:"items,omitempty"`
	Map     MapSpec        `json:"map"`
	Reduce  *ReduceSpec    `json:"reduce,omitempty"`
	Context map[string]any `json:"context,omitempty"`
	Inputs  map[string]any `json:"inputs,omitempty"`
}

// MapSpec configures the map stage.
type MapSpec struct {
	StoryRef                 *refs.ObjectReference `json:"storyRef,omitempty"`
	Story                    map[string]any        `json:"story,omitempty"`
	Inputs                   map[string]any        `json:"inputs,omitempty"`
	BatchSize                *int                  `json:"batchSize,omitempty"`
	Concurrency              *int                  `json:"concurrency,omitempty"`
	FailFast                 *bool                 `json:"failFast,omitempty"`
	AllowFailures            *bool                 `json:"allowFailures,omitempty"`
	InlineResultsLimit       *int                  `json:"inlineResultsLimit,omitempty"`
	PollInterval             *time.Duration        `json:"pollInterval,omitempty"`
	StoryRunTTLSeconds       *int32                `json:"storyRunTTLSeconds,omitempty"`
	StoryRunRetentionSeconds *int32                `json:"storyRunRetentionSeconds,omitempty"`
}

// ReduceSpec configures the optional reduce stage.
type ReduceSpec struct {
	StoryRef                 *refs.ObjectReference `json:"storyRef,omitempty"`
	Story                    map[string]any        `json:"story,omitempty"`
	Inputs                   map[string]any        `json:"inputs,omitempty"`
	PollInterval             *time.Duration        `json:"pollInterval,omitempty"`
	StoryRunTTLSeconds       *int32                `json:"storyRunTTLSeconds,omitempty"`
	StoryRunRetentionSeconds *int32                `json:"storyRunRetentionSeconds,omitempty"`
}
