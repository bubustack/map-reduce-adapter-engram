package engram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/runtime/identity"
	"github.com/bubustack/map-reduce-adapter-engram/pkg/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

const (
	defaultBatchSize           = 100
	defaultConcurrency         = 10
	defaultPollInterval        = 2 * time.Second
	defaultInlineResultsLimit  = 0
	defaultFailFast            = true
	defaultAllowFailures       = false
	manifestVersion            = "v1"
	manifestFileName           = "map-manifest.json"
	resultsIndexFileName       = "results-index.json"
	reduceOutputFileName       = "reduce-output.json"
	mapItemOutputDir           = "map-items"
	mapItemOutputFilePattern   = "item-%06d.json"
	createdStoryNamePrefixBase = "mapreduce"
)

var (
	mapReservedInputKeys = map[string]struct{}{
		"item":    {},
		"index":   {},
		"total":   {},
		"context": {},
		"inputs":  {},
	}
	reduceReservedInputKeys = map[string]struct{}{
		"manifest":      {},
		"resultsIndex":  {},
		"resultsPrefix": {},
		"stats":         {},
	}
)

type Adapter struct {
	config config.Config
}

type AdapterOutput struct {
	Manifest      map[string]any `json:"manifest"`
	Stats         ManifestStats  `json:"stats"`
	ResultsPrefix string         `json:"resultsPrefix"`
	ResultsIndex  map[string]any `json:"resultsIndex"`
	Results       []any          `json:"results,omitempty"`
	Reduce        *ReduceResult  `json:"reduce,omitempty"`
}

type MapResult struct {
	Index     int            `json:"index"`
	StoryRun  string         `json:"storyRun"`
	Phase     enums.Phase    `json:"phase"`
	OutputRef map[string]any `json:"outputRef,omitempty"`
	Error     string         `json:"error,omitempty"`
}

type ManifestStats struct {
	Total     int `json:"total"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
}

type MapManifest struct {
	Version  string         `json:"version"`
	StoryRef map[string]any `json:"storyRef"`
	Stats    ManifestStats  `json:"stats"`
	Items    []MapResult    `json:"items"`
}

type MapResultsIndex struct {
	Version string        `json:"version"`
	Stats   ManifestStats `json:"stats"`
	Items   []MapResult   `json:"items"`
}

type ReduceResult struct {
	StoryRun  string         `json:"storyRun"`
	Phase     enums.Phase    `json:"phase"`
	OutputRef map[string]any `json:"outputRef,omitempty"`
	Error     string         `json:"error,omitempty"`
}

// New returns a ready-to-initialize adapter instance.
func New() *Adapter {
	return &Adapter{}
}

// Init validates configuration and applies defaults.
func (a *Adapter) Init(_ context.Context, cfg config.Config, _ *engram.Secrets) error {
	cfg.DefaultBatchSize = firstPositive(cfg.DefaultBatchSize, defaultBatchSize)
	cfg.DefaultConcurrency = firstPositive(cfg.DefaultConcurrency, defaultConcurrency)
	cfg.DefaultInlineResultsLimit = maxZero(cfg.DefaultInlineResultsLimit)
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.DefaultFailFast == nil {
		val := defaultFailFast
		cfg.DefaultFailFast = &val
	}
	if cfg.DefaultAllowFailures == nil {
		val := defaultAllowFailures
		cfg.DefaultAllowFailures = &val
	}
	if cfg.MaxBatchSize < 0 {
		return fmt.Errorf("maxBatchSize cannot be negative")
	}
	if cfg.MaxConcurrency < 0 {
		return fmt.Errorf("maxConcurrency cannot be negative")
	}
	if cfg.MaxBatchSize > 0 && cfg.DefaultBatchSize > cfg.MaxBatchSize {
		return fmt.Errorf("defaultBatchSize cannot exceed maxBatchSize")
	}
	if cfg.MaxConcurrency > 0 && cfg.DefaultConcurrency > cfg.MaxConcurrency {
		return fmt.Errorf("defaultConcurrency cannot exceed maxConcurrency")
	}
	a.config = cfg
	return nil
}

func (a *Adapter) Process(
	ctx context.Context,
	execCtx *engram.ExecutionContext,
	inputs config.Inputs,
) (*engram.Result, error) {
	logger := execCtx.Logger()
	if err := validateAdapterInputs(inputs); err != nil {
		return nil, err
	}

	k8sClient, sm, err := newAdapterRuntime(ctx)
	if err != nil {
		return nil, err
	}

	items, err := resolveItems(ctx, sm, inputs.Items)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve items: %w", err)
	}
	logResolvedItems(logger, items)

	mapSpec := inputs.Map
	mapStoryRef, mapStoryName, err := resolveStoryRef(ctx, k8sClient, execCtx, mapSpec, a.config)
	if err != nil {
		return nil, err
	}

	options := a.resolveExecutionOptions(mapSpec, len(items))

	logger.Info("Starting map-reduce adapter",
		"items", len(items),
		"story", mapStoryName,
		"batchSize", options.batchSize,
		"concurrency", options.concurrency,
		"failFast", options.failFast,
	)

	results, mapErr := a.executeMapStage(
		ctx,
		k8sClient,
		sm,
		execCtx,
		mapStoryRef,
		mapSpec,
		inputs,
		items,
		options,
	)

	stats := summarizeResults(results)
	manifestRef, resultsIndexRef, err := a.storeStageArtifacts(ctx, sm, execCtx, mapStoryRef, stats, results)
	if err != nil {
		return nil, err
	}

	var reduceResult *ReduceResult
	if mapErr == nil && (stats.Failed == 0 || options.allowFailures) && inputs.Reduce != nil {
		reduceResult, err = a.runReduce(
			ctx,
			k8sClient,
			sm,
			execCtx,
			*inputs.Reduce,
			manifestRef,
			resultsIndexRef,
			stats,
		)
		if err != nil {
			mapErr = err
		}
	}

	finalErr := mapErr
	if finalErr == nil && stats.Failed > 0 && !options.allowFailures {
		finalErr = errors.New("map stage had failures")
	}
	resultsPrefix := mapItemsPrefix(execCtx.StoryInfo())
	return a.finalizeOutput(
		ctx,
		sm,
		execCtx,
		resultsPrefix,
		manifestRef,
		resultsIndexRef,
		results,
		options.inlineLimit,
		reduceResult,
		finalErr,
	)
}

type executionOptions struct {
	batchSize     int
	concurrency   int
	inlineLimit   int
	failFast      bool
	allowFailures bool
	pollInterval  time.Duration
}

func validateAdapterInputs(inputs config.Inputs) error {
	if err := validateMapSpec(inputs.Map); err != nil {
		return err
	}
	if err := validateReservedInputKeys("map", inputs.Map.Inputs, mapReservedInputKeys); err != nil {
		return err
	}
	if inputs.Reduce != nil {
		if err := validateReservedInputKeys("reduce", inputs.Reduce.Inputs, reduceReservedInputKeys); err != nil {
			return err
		}
	}
	if inputs.Items == nil {
		return fmt.Errorf("items is required")
	}
	return nil
}

func newAdapterRuntime(ctx context.Context) (*k8s.Client, *storage.StorageManager, error) {
	k8sClient, err := k8s.NewClient()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create k8s client: %w", err)
	}
	sm, err := storage.SharedManager(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create storage manager: %w", err)
	}
	if sm == nil || sm.GetStore() == nil {
		return nil, nil, fmt.Errorf("storage backend is required for map-reduce adapter outputs")
	}
	return k8sClient, sm, nil
}

func (a *Adapter) resolveExecutionOptions(mapSpec config.MapSpec, itemCount int) executionOptions {
	options := executionOptions{
		batchSize:     applyBounds(resolveInt(mapSpec.BatchSize, a.config.DefaultBatchSize), a.config.MaxBatchSize),
		concurrency:   applyBounds(resolveInt(mapSpec.Concurrency, a.config.DefaultConcurrency), a.config.MaxConcurrency),
		inlineLimit:   resolveInt(mapSpec.InlineResultsLimit, a.config.DefaultInlineResultsLimit),
		failFast:      resolveBool(mapSpec.FailFast, a.config.DefaultFailFast),
		allowFailures: resolveBool(mapSpec.AllowFailures, a.config.DefaultAllowFailures),
		pollInterval:  resolveDuration(mapSpec.PollInterval, a.config.PollInterval),
	}
	if options.batchSize <= 0 {
		options.batchSize = itemCount
	}
	if options.concurrency <= 0 {
		options.concurrency = defaultConcurrency
	}
	if itemCount > 0 && options.concurrency > itemCount {
		options.concurrency = itemCount
	}
	return options
}

func (a *Adapter) executeMapStage(
	ctx context.Context,
	k8sClient *k8s.Client,
	sm *storage.StorageManager,
	execCtx *engram.ExecutionContext,
	mapStoryRef *refs.ObjectReference,
	mapSpec config.MapSpec,
	inputs config.Inputs,
	items []any,
	options executionOptions,
) ([]MapResult, error) {
	results := initMapResults(len(items))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var mapErr error
	for batchStart := 0; batchStart < len(items); batchStart += options.batchSize {
		batchEnd := min(batchStart+options.batchSize, len(items))
		if err := a.processBatch(
			ctx,
			cancel,
			k8sClient,
			sm,
			execCtx,
			mapStoryRef,
			mapSpec,
			inputs,
			items,
			results,
			batchStart,
			batchEnd,
			options.concurrency,
			options.pollInterval,
			options.failFast,
		); err != nil {
			mapErr = err
			break
		}
		if options.failFast && ctx.Err() != nil {
			mapErr = ctx.Err()
			break
		}
	}
	return results, mapErr
}

func initMapResults(count int) []MapResult {
	results := make([]MapResult, count)
	for i := range results {
		results[i] = MapResult{Index: i, Phase: enums.PhaseSkipped, Error: "not started"}
	}
	return results
}

func sampleItems(items []any, max int) []any {
	if max <= 0 || len(items) == 0 {
		return nil
	}
	if len(items) > max {
		items = items[:max]
	}
	sample := make([]any, 0, len(items))
	for _, item := range items {
		switch v := item.(type) {
		case map[string]any:
			entry := map[string]any{}
			if name, ok := v["name"]; ok {
				entry["name"] = name
			}
			if url, ok := v["url"]; ok {
				entry["url"] = url
			}
			if len(entry) == 0 {
				entry = v
			}
			sample = append(sample, entry)
		default:
			sample = append(sample, v)
		}
	}
	return sample
}

func logResolvedItems(logger *slog.Logger, items []any) {
	if logger == nil {
		return
	}
	sample := sampleItems(items, 3)
	if len(sample) == 0 {
		return
	}
	logger.Info("Resolved map items", "items", len(items), "sample", sample)
}

func (a *Adapter) processBatch(
	ctx context.Context,
	cancel context.CancelFunc,
	k8sClient *k8s.Client,
	sm *storage.StorageManager,
	execCtx *engram.ExecutionContext,
	storyRef *refs.ObjectReference,
	mapSpec config.MapSpec,
	inputs config.Inputs,
	items []any,
	results []MapResult,
	start int,
	end int,
	concurrency int,
	pollInterval time.Duration,
	failFast bool,
) error {
	if start >= end {
		return nil
	}

	sem := make(chan struct{}, concurrency)
	resultCh := make(chan MapResult, end-start)
	var wg sync.WaitGroup

	for idx := start; idx < end; idx++ {
		if ctx.Err() != nil {
			break
		}
		item := items[idx]
		sem <- struct{}{}
		wg.Add(1)
		go func(index int, val any) {
			defer wg.Done()
			defer func() { <-sem }()
			res := a.processItem(ctx, k8sClient, sm, execCtx, storyRef, mapSpec, inputs, val, index, len(items), pollInterval)
			if failFast && res.Error != "" {
				cancel()
			}
			resultCh <- res
		}(idx, item)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for res := range resultCh {
		if res.Index >= 0 && res.Index < len(results) {
			results[res.Index] = res
		}
	}

	if ctx.Err() != nil && failFast {
		return ctx.Err()
	}
	return nil
}

func (a *Adapter) processItem(
	ctx context.Context,
	k8sClient *k8s.Client,
	sm *storage.StorageManager,
	execCtx *engram.ExecutionContext,
	storyRef *refs.ObjectReference,
	mapSpec config.MapSpec,
	inputs config.Inputs,
	item any,
	index int,
	total int,
	pollInterval time.Duration,
) MapResult {
	payload, err := buildMapPayload(inputs, mapSpec, item, index, total)
	if err != nil {
		return MapResult{Index: index, Phase: enums.PhaseFailed, Error: err.Error()}
	}

	stepInfo := execCtx.StoryInfo()
	token := fmt.Sprintf("%s-%d", stepInfo.StepRunID, index)
	ctxWithToken := k8s.WithTriggerToken(ctx, token)

	storyRun, err := k8sClient.TriggerStory(ctxWithToken, storyRef.Name, derefString(storyRef.Namespace), payload)
	if err != nil {
		return MapResult{Index: index, Phase: enums.PhaseFailed, Error: err.Error()}
	}

	completed, err := waitForStoryRun(ctx, k8sClient, storyRun, pollInterval)
	if err != nil {
		return MapResult{Index: index, StoryRun: storyRun.Name, Phase: enums.PhaseFailed, Error: err.Error()}
	}

	result := MapResult{Index: index, StoryRun: completed.Name, Phase: completed.Status.Phase}
	if completed.Status.Phase != enums.PhaseSucceeded {
		msg := strings.TrimSpace(completed.Status.Message)
		if msg == "" {
			msg = fmt.Sprintf("storyrun %s finished with phase %s", completed.Name, completed.Status.Phase)
		}
		result.Error = msg
		return result
	}

	if completed.Status.Output == nil || len(completed.Status.Output.Raw) == 0 {
		return result
	}

	outputPath := mapItemOutputPath(stepInfo, index)
	ref, err := storeOutputRef(ctx, sm, outputPath, completed.Status.Output.Raw)
	if err != nil {
		result.Error = err.Error()
		result.Phase = enums.PhaseFailed
		return result
	}
	result.OutputRef = ref
	return result
}

func buildMapPayload(
	inputs config.Inputs,
	mapSpec config.MapSpec,
	item any,
	index int,
	total int,
) (map[string]any, error) {
	if err := validateReservedInputKeys("map", mapSpec.Inputs, mapReservedInputKeys); err != nil {
		return nil, err
	}
	payload := map[string]any{
		"item":  item,
		"index": index,
		"total": total,
	}
	if inputs.Context != nil {
		payload["context"] = inputs.Context
	}
	if len(inputs.Inputs) > 0 {
		payload["inputs"] = inputs.Inputs
	}
	for k, v := range mapSpec.Inputs {
		payload[k] = v
	}
	return payload, nil
}

func buildReducePayload(
	reduce config.ReduceSpec,
	manifestRef map[string]any,
	resultsIndexRef map[string]any,
	resultsPrefix string,
	stats ManifestStats,
) (map[string]any, error) {
	if err := validateReservedInputKeys("reduce", reduce.Inputs, reduceReservedInputKeys); err != nil {
		return nil, err
	}
	payload := map[string]any{
		"manifest":      manifestRef,
		"resultsIndex":  resultsIndexRef,
		"resultsPrefix": resultsPrefix,
		"stats":         stats,
	}
	for k, v := range reduce.Inputs {
		payload[k] = v
	}
	return payload, nil
}

func validateReservedInputKeys(stage string, values map[string]any, reserved map[string]struct{}) error {
	if len(values) == 0 {
		return nil
	}
	for key := range values {
		if _, exists := reserved[key]; exists {
			return fmt.Errorf("%s.inputs key %q is reserved and cannot be overridden", stage, key)
		}
	}
	return nil
}

func (a *Adapter) runReduce(
	ctx context.Context,
	k8sClient *k8s.Client,
	sm *storage.StorageManager,
	execCtx *engram.ExecutionContext,
	reduce config.ReduceSpec,
	manifestRef map[string]any,
	resultsIndexRef map[string]any,
	stats ManifestStats,
) (*ReduceResult, error) {
	if reduce.StoryRef == nil && reduce.Story == nil {
		return nil, fmt.Errorf("reduce.storyRef or reduce.story is required")
	}
	if reduce.StoryRef != nil && reduce.Story != nil {
		return nil, fmt.Errorf("reduce.storyRef and reduce.story cannot both be set")
	}

	ref, name, err := resolveStoryRef(ctx, k8sClient, execCtx, config.MapSpec{
		StoryRef:                 reduce.StoryRef,
		Story:                    reduce.Story,
		StoryRunTTLSeconds:       reduce.StoryRunTTLSeconds,
		StoryRunRetentionSeconds: reduce.StoryRunRetentionSeconds,
	}, a.config)
	if err != nil {
		return &ReduceResult{Error: err.Error()}, err
	}

	payload, err := buildReducePayload(
		reduce,
		manifestRef,
		resultsIndexRef,
		mapItemsPrefix(execCtx.StoryInfo()),
		stats,
	)
	if err != nil {
		return &ReduceResult{Phase: enums.PhaseFailed, Error: err.Error()}, err
	}

	stepInfo := execCtx.StoryInfo()
	token := fmt.Sprintf("%s-reduce", stepInfo.StepRunID)
	ctxWithToken := k8s.WithTriggerToken(ctx, token)
	storyRun, err := k8sClient.TriggerStory(ctxWithToken, ref.Name, derefString(ref.Namespace), payload)
	if err != nil {
		return &ReduceResult{Phase: enums.PhaseFailed, Error: err.Error()}, err
	}

	pollInterval := resolveDuration(reduce.PollInterval, a.config.PollInterval)
	completed, err := waitForStoryRun(ctx, k8sClient, storyRun, pollInterval)
	if err != nil {
		return &ReduceResult{StoryRun: storyRun.Name, Phase: enums.PhaseFailed, Error: err.Error()}, err
	}

	result := &ReduceResult{StoryRun: completed.Name, Phase: completed.Status.Phase}
	if completed.Status.Phase != enums.PhaseSucceeded {
		msg := strings.TrimSpace(completed.Status.Message)
		if msg == "" {
			msg = fmt.Sprintf("reduce story %s finished with phase %s", name, completed.Status.Phase)
		}
		result.Error = msg
		return result, errors.New(msg)
	}

	if completed.Status.Output == nil || len(completed.Status.Output.Raw) == 0 {
		return result, nil
	}
	outputPath := reduceOutputPath(stepInfo)
	refMap, err := storeOutputRef(ctx, sm, outputPath, completed.Status.Output.Raw)
	if err != nil {
		result.Error = err.Error()
		return result, err
	}
	result.OutputRef = refMap
	return result, nil
}

func (a *Adapter) storeStageArtifacts(
	ctx context.Context,
	sm *storage.StorageManager,
	execCtx *engram.ExecutionContext,
	mapStoryRef *refs.ObjectReference,
	stats ManifestStats,
	results []MapResult,
) (map[string]any, map[string]any, error) {
	manifestRef, err := a.storeManifest(ctx, sm, execCtx, mapStoryRef, stats, results)
	if err != nil {
		return nil, nil, err
	}
	resultsIndexRef, err := a.storeResultsIndex(ctx, sm, execCtx, stats, results)
	if err != nil {
		return nil, nil, err
	}
	return manifestRef, resultsIndexRef, nil
}

func (a *Adapter) storeManifest(
	ctx context.Context,
	sm *storage.StorageManager,
	execCtx *engram.ExecutionContext,
	storyRef *refs.ObjectReference,
	stats ManifestStats,
	results []MapResult,
) (map[string]any, error) {
	manifest := MapManifest{
		Version:  manifestVersion,
		StoryRef: storyRefToMap(storyRef),
		Stats:    stats,
		Items:    results,
	}

	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest: %w", err)
	}
	manifestPath := manifestPath(execCtx.StoryInfo())
	if err := writeStoredObject(ctx, sm, manifestPath, manifestBytes); err != nil {
		return nil, fmt.Errorf("failed to store manifest: %w", err)
	}
	return map[string]any{storage.StorageRefKey: manifestPath}, nil
}

func (a *Adapter) storeResultsIndex(
	ctx context.Context,
	sm *storage.StorageManager,
	execCtx *engram.ExecutionContext,
	stats ManifestStats,
	results []MapResult,
) (map[string]any, error) {
	index := MapResultsIndex{
		Version: manifestVersion,
		Stats:   stats,
		Items:   results,
	}
	indexBytes, err := json.Marshal(index)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal results index: %w", err)
	}
	indexPath := resultsIndexPath(execCtx.StoryInfo())
	if err := writeStoredObject(ctx, sm, indexPath, indexBytes); err != nil {
		return nil, fmt.Errorf("failed to store results index: %w", err)
	}
	return map[string]any{storage.StorageRefKey: indexPath}, nil
}

func (a *Adapter) finalizeOutput(
	ctx context.Context,
	sm *storage.StorageManager,
	execCtx *engram.ExecutionContext,
	resultsPrefix string,
	manifestRef map[string]any,
	resultsIndexRef map[string]any,
	results []MapResult,
	inlineLimit int,
	reduce *ReduceResult,
	finalErr error,
) (*engram.Result, error) {
	logger := execCtx.Logger()
	stats := summarizeResults(results)
	output := AdapterOutput{
		Manifest:      manifestRef,
		Stats:         stats,
		ResultsPrefix: resultsPrefix,
		ResultsIndex:  resultsIndexRef,
		Reduce:        reduce,
	}

	if inlineLimit > 0 && len(results) > 0 && len(results) <= inlineLimit {
		inline := make([]any, 0, len(results))
		for _, res := range results {
			if res.OutputRef == nil {
				inline = append(inline, nil)
				continue
			}
			value, err := hydrateOutputRef(ctx, sm, execCtx, res.OutputRef)
			if err != nil {
				if logger != nil {
					logger.Warn("Failed to hydrate inline map output; falling back to storage ref", "index", res.Index, "error", err)
				}
				inline = append(inline, res.OutputRef)
				continue
			}
			inline = append(inline, value)
		}
		output.Results = inline
	}

	return engram.NewResultFrom(output), finalErr
}

func summarizeResults(results []MapResult) ManifestStats {
	stats := ManifestStats{Total: len(results)}
	for _, res := range results {
		switch res.Phase {
		case enums.PhaseSucceeded:
			stats.Succeeded++
		case "":
			continue
		default:
			stats.Failed++
		}
	}
	return stats
}

func resolveStoryRef(
	ctx context.Context,
	k8sClient *k8s.Client,
	execCtx *engram.ExecutionContext,
	spec config.MapSpec,
	cfg config.Config,
) (*refs.ObjectReference, string, error) {
	if spec.StoryRef != nil {
		name := spec.StoryRef.Name
		if name == "" {
			return nil, "", fmt.Errorf("storyRef.name is required")
		}
		return spec.StoryRef, name, nil
	}

	storySpec, err := decodeStorySpec(spec.Story)
	if err != nil {
		return nil, "", err
	}
	applyStoryDefaults(storySpec, spec, cfg)

	stepInfo := execCtx.StoryInfo()
	storyNamePrefix := sanitizeName(fmt.Sprintf("%s-%s-", createdStoryNamePrefixBase, stepInfo.StepName))
	if storyNamePrefix == "" {
		storyNamePrefix = createdStoryNamePrefixBase + "-"
	}

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: storyNamePrefix,
			Namespace:    stepInfo.StepRunNamespace,
			Labels: map[string]string{
				"bubustack.io/adapter": "map-reduce",
				contracts.StepLabelKey: identity.LabelValueFromName(stepInfo.StepName),
			},
			Annotations: map[string]string{
				contracts.StoryRunAnnotation: stepInfo.StoryRunID,
				contracts.StepAnnotation:     stepInfo.StepName,
				contracts.StepRunLabelKey:    stepInfo.StepRunID,
			},
		},
		Spec: *storySpec,
	}
	if storyRun := strings.TrimSpace(stepInfo.StoryRunID); storyRun != "" {
		story.Labels[contracts.StoryRunLabelKey] = identity.LabelValueFromName(storyRun)
		story.Labels[contracts.ParentStoryRunLabel] = identity.SafeLabelValue(storyRun)
		story.Annotations[contracts.ParentStoryRunLabel] = storyRun
	}
	if stepRun := strings.TrimSpace(stepInfo.StepRunID); stepRun != "" {
		story.Labels[contracts.StepRunLabelKey] = identity.LabelValueFromName(stepRun)
		story.Annotations["bubustack.io/parent-steprun"] = stepRun
	}
	if step := strings.TrimSpace(stepInfo.StepName); step != "" {
		story.Labels[contracts.ParentStepLabel] = identity.SafeLabelValue(step)
		story.Annotations[contracts.ParentStepLabel] = step
	}

	ownerRef, err := stepRunOwnerRef(ctx, k8sClient, stepInfo)
	if err != nil {
		return nil, "", fmt.Errorf("failed to resolve StepRun owner reference for inline story cleanup: %w", err)
	}
	story.OwnerReferences = []metav1.OwnerReference{*ownerRef}

	if err := k8sClient.Create(ctx, story); err != nil {
		return nil, "", fmt.Errorf("failed to create inline story: %w", err)
	}

	ref := &refs.ObjectReference{Name: story.Name}
	if story.Namespace != "" {
		ns := story.Namespace
		ref.Namespace = &ns
	}

	return ref, story.Name, nil
}

func decodeStorySpec(raw map[string]any) (*bubuv1alpha1.StorySpec, error) {
	if raw == nil {
		return nil, fmt.Errorf("story spec is required")
	}
	payload, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to encode story spec: %w", err)
	}
	var spec bubuv1alpha1.StorySpec
	if err := json.Unmarshal(payload, &spec); err != nil {
		return nil, fmt.Errorf("failed to decode story spec: %w", err)
	}
	if len(spec.Steps) == 0 {
		return nil, fmt.Errorf("story spec must include steps")
	}
	return &spec, nil
}

func applyStoryDefaults(spec *bubuv1alpha1.StorySpec, mapSpec config.MapSpec, cfg config.Config) {
	if spec.Pattern == "" {
		spec.Pattern = enums.BatchPattern
	}
	if spec.Policy == nil {
		spec.Policy = &bubuv1alpha1.StoryPolicy{}
	}
	if spec.Policy.Execution == nil {
		spec.Policy.Execution = &bubuv1alpha1.ExecutionPolicy{}
	}
	if spec.Policy.Execution.Job == nil {
		spec.Policy.Execution.Job = &bubuv1alpha1.JobPolicy{}
	}
	job := spec.Policy.Execution.Job
	if job.TTLSecondsAfterFinished == nil {
		job.TTLSecondsAfterFinished = firstNonNil(mapSpec.StoryRunTTLSeconds, cfg.DefaultStoryRunTTLSeconds)
	}
	if job.StoryRunRetentionSeconds == nil {
		job.StoryRunRetentionSeconds = firstNonNil(mapSpec.StoryRunRetentionSeconds, cfg.DefaultStoryRunRetentionSeconds)
	}
}

func stepRunOwnerRef(
	ctx context.Context,
	k8sClient *k8s.Client,
	info engram.StoryInfo,
) (*metav1.OwnerReference, error) {
	if info.StepRunID == "" || info.StepRunNamespace == "" {
		return nil, fmt.Errorf("step run metadata missing")
	}
	stepRun := &runsv1alpha1.StepRun{}
	key := types.NamespacedName{Name: info.StepRunID, Namespace: info.StepRunNamespace}
	if err := k8sClient.Get(ctx, key, stepRun); err != nil {
		return nil, err
	}
	return &metav1.OwnerReference{
		APIVersion:         runsv1alpha1.GroupVersion.String(),
		Kind:               "StepRun",
		Name:               stepRun.Name,
		UID:                stepRun.UID,
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(true),
	}, nil
}

func waitForStoryRun(
	ctx context.Context,
	k8sClient *k8s.Client,
	run *runsv1alpha1.StoryRun,
	pollInterval time.Duration,
) (*runsv1alpha1.StoryRun, error) {
	if run == nil {
		return nil, fmt.Errorf("storyrun is nil")
	}
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		latest := &runsv1alpha1.StoryRun{}
		key := types.NamespacedName{Name: run.Name, Namespace: run.Namespace}
		if err := k8sClient.Get(ctx, key, latest); err != nil {
			return nil, err
		}
		if latest.Status.Phase.IsTerminal() {
			return latest, nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func mapItemOutputPath(info engram.StoryInfo, index int) string {
	file := fmt.Sprintf(mapItemOutputFilePattern, index)
	return path.Join(mapItemsPrefix(info), file)
}

func mapItemsPrefix(info engram.StoryInfo) string {
	stepKey := storage.NamespacedKey(info.StepRunNamespace, info.StepRunID)
	prefix := outputPrefix()
	return path.Join(prefix, stepKey, mapItemOutputDir)
}

func reduceOutputPath(info engram.StoryInfo) string {
	stepKey := storage.NamespacedKey(info.StepRunNamespace, info.StepRunID)
	prefix := outputPrefix()
	return path.Join(prefix, stepKey, reduceOutputFileName)
}

func manifestPath(info engram.StoryInfo) string {
	stepKey := storage.NamespacedKey(info.StepRunNamespace, info.StepRunID)
	prefix := outputPrefix()
	return path.Join(prefix, stepKey, manifestFileName)
}

func resultsIndexPath(info engram.StoryInfo) string {
	stepKey := storage.NamespacedKey(info.StepRunNamespace, info.StepRunID)
	prefix := outputPrefix()
	return path.Join(prefix, stepKey, resultsIndexFileName)
}

func outputPrefix() string {
	if v := strings.TrimSpace(os.Getenv(contracts.StorageS3OutputPrefixEnv)); v != "" {
		return v
	}
	return "outputs"
}

func storeOutputRef(
	ctx context.Context,
	sm *storage.StorageManager,
	objectPath string,
	raw []byte,
) (map[string]any, error) {
	if sm == nil || sm.GetStore() == nil {
		return nil, fmt.Errorf("storage backend is required")
	}
	if raw == nil {
		raw = []byte("null")
	}
	if err := writeStoredObject(ctx, sm, objectPath, raw); err != nil {
		return nil, err
	}
	return map[string]any{storage.StorageRefKey: objectPath}, nil
}

func writeStoredObject(
	ctx context.Context,
	sm *storage.StorageManager,
	objectPath string,
	payload []byte,
) error {
	stored := storage.StoredObject{
		ContentType: "json",
		Data:        payload,
	}
	encoded, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("failed to encode stored object: %w", err)
	}
	return sm.WriteBlob(ctx, objectPath, "application/json", encoded)
}

func hydrateOutputRef(
	ctx context.Context,
	sm *storage.StorageManager,
	execCtx *engram.ExecutionContext,
	ref map[string]any,
) (any, error) {
	if sm == nil || sm.GetStore() == nil {
		return nil, fmt.Errorf("storage backend is required")
	}
	info := execCtx.StoryInfo()
	stepKey := storage.NamespacedKey(info.StepRunNamespace, info.StepRunID)
	hctx := storage.WithStepRunID(ctx, stepKey)
	return sm.Hydrate(hctx, ref)
}

func validateMapSpec(spec config.MapSpec) error {
	if spec.StoryRef == nil && spec.Story == nil {
		return fmt.Errorf("map.storyRef or map.story is required")
	}
	if spec.StoryRef != nil && spec.Story != nil {
		return fmt.Errorf("map.storyRef and map.story cannot both be set")
	}
	if spec.BatchSize != nil && *spec.BatchSize <= 0 {
		return fmt.Errorf("map.batchSize must be greater than 0 when set")
	}
	if spec.Concurrency != nil && *spec.Concurrency <= 0 {
		return fmt.Errorf("map.concurrency must be greater than 0 when set")
	}
	if spec.InlineResultsLimit != nil && *spec.InlineResultsLimit < 0 {
		return fmt.Errorf("map.inlineResultsLimit cannot be negative")
	}
	if spec.PollInterval != nil && *spec.PollInterval < 0 {
		return fmt.Errorf("map.pollInterval cannot be negative")
	}
	return nil
}

func resolveItems(ctx context.Context, sm *storage.StorageManager, raw any) ([]any, error) {
	if raw == nil {
		return nil, fmt.Errorf("items cannot be null")
	}
	if sm == nil {
		return nil, fmt.Errorf("storage manager is required to resolve items")
	}
	hydrated, err := sm.Hydrate(ctx, raw)
	if err != nil {
		return nil, err
	}
	return coerceItems(hydrated)
}

func coerceItems(value any) ([]any, error) {
	switch typed := value.(type) {
	case string:
		return decodeItemsJSON([]byte(typed), "string")
	case []byte:
		return decodeItemsJSON(typed, "bytes")
	case []any:
		return typed, nil
	case []string:
		return sliceToAny(typed), nil
	case []int:
		return sliceToAny(typed), nil
	case []int32:
		return sliceToAny(typed), nil
	case []int64:
		return sliceToAny(typed), nil
	case []float32:
		return sliceToAny(typed), nil
	case []float64:
		return sliceToAny(typed), nil
	case []bool:
		return sliceToAny(typed), nil
	case []map[string]any:
		return sliceToAny(typed), nil
	default:
		return nil, fmt.Errorf("items must be a list, got %T", value)
	}
}

func decodeItemsJSON(raw []byte, source string) ([]any, error) {
	var decoded []any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("items must be a list, got %s: %w", source, err)
	}
	return decoded, nil
}

func sliceToAny[T any](items []T) []any {
	out := make([]any, len(items))
	for i, item := range items {
		out[i] = item
	}
	return out
}

func resolveInt(value *int, fallback int) int {
	if value != nil {
		return *value
	}
	return fallback
}

func resolveBool(value *bool, fallback *bool) bool {
	if value != nil {
		return *value
	}
	if fallback != nil {
		return *fallback
	}
	return false
}

func resolveDuration(value *time.Duration, fallback time.Duration) time.Duration {
	if value != nil && *value > 0 {
		return *value
	}
	return fallback
}

func applyBounds(value int, max int) int {
	if max > 0 && value > max {
		return max
	}
	return value
}

func firstPositive(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func maxZero(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func storyRefToMap(ref *refs.ObjectReference) map[string]any {
	if ref == nil {
		return map[string]any{}
	}
	out := map[string]any{"name": ref.Name}
	if ref.Namespace != nil {
		out["namespace"] = *ref.Namespace
	}
	return out
}

func sanitizeName(name string) string {
	clean := strings.ToLower(strings.TrimSpace(name))
	clean = strings.ReplaceAll(clean, "_", "-")
	clean = strings.ReplaceAll(clean, " ", "-")
	clean = strings.Trim(clean, "-")
	if clean == "" {
		return ""
	}
	if len(clean) > 40 {
		clean = clean[:40]
	}
	return clean
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func firstNonNil(primary *int32, fallback *int32) *int32 {
	if primary != nil {
		return primary
	}
	return fallback
}
