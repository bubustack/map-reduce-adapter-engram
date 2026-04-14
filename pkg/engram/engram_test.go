package engram

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	sdkengram "github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/map-reduce-adapter-engram/pkg/config"
)

func TestBuildMapPayloadRejectsReservedInputKey(t *testing.T) {
	_, err := buildMapPayload(
		config.Inputs{},
		config.MapSpec{Inputs: map[string]any{"item": "override"}},
		"value",
		0,
		1,
	)
	if err == nil {
		t.Fatal("expected reserved key validation error")
	}
	if !strings.Contains(err.Error(), `map.inputs key "item" is reserved`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildMapPayloadUsesReservedValues(t *testing.T) {
	payload, err := buildMapPayload(
		config.Inputs{
			Context: map[string]any{"env": "dev"},
			Inputs:  map[string]any{"tenant": "alpha"},
		},
		config.MapSpec{
			Inputs: map[string]any{"custom": "value"},
		},
		map[string]any{"id": 42},
		3,
		9,
	)
	if err != nil {
		t.Fatalf("buildMapPayload returned error: %v", err)
	}
	if payload["index"] != 3 || payload["total"] != 9 {
		t.Fatalf("expected index/total in payload, got %#v", payload)
	}
	if payload["custom"] != "value" {
		t.Fatalf("expected custom input preserved, got %#v", payload["custom"])
	}
}

func TestBuildReducePayloadRejectsReservedInputKey(t *testing.T) {
	_, err := buildReducePayload(
		config.ReduceSpec{Inputs: map[string]any{"manifest": "override"}},
		map[string]any{"$bubuStorageRef": "manifest.json"},
		map[string]any{"$bubuStorageRef": "index.json"},
		"outputs/ns/run/map-items",
		ManifestStats{},
	)
	if err == nil {
		t.Fatal("expected reserved key validation error")
	}
	if !strings.Contains(err.Error(), `reduce.inputs key "manifest" is reserved`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveStoryRefInlineRequiresOwnerReference(t *testing.T) {
	execCtx := sdkengram.NewExecutionContext(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		sdkengram.StoryInfo{},
	)

	_, _, err := resolveStoryRef(
		context.Background(),
		nil,
		execCtx,
		config.MapSpec{
			Story: map[string]any{
				"steps": []any{
					map[string]any{
						"name": "step-1",
						"ref": map[string]any{
							"name": "noop",
						},
					},
				},
			},
		},
		config.Config{},
	)
	if err == nil {
		t.Fatal("expected owner reference resolution error")
	}
	if !strings.Contains(err.Error(), "failed to resolve StepRun owner reference for inline story cleanup") {
		t.Fatalf("unexpected error: %v", err)
	}
}
