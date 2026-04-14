package main

import (
	"os"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestEngramTemplateInputSchemaUsesObjectRequiredList(t *testing.T) {
	raw, err := os.ReadFile("Engram.yaml")
	if err != nil {
		t.Fatalf("read Engram.yaml: %v", err)
	}

	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal Engram.yaml: %v", err)
	}

	spec := getMap(t, doc, "spec")
	inputSchema := getMap(t, spec, "inputSchema")
	required := getSlice(t, inputSchema, "required")
	if !containsString(required, "items") || !containsString(required, "map") {
		t.Fatalf("inputSchema.required must include items and map, got %#v", required)
	}

	properties := getMap(t, inputSchema, "properties")
	items := getMap(t, properties, "items")
	if _, ok := items["required"]; ok {
		t.Fatalf("items property must not define scalar required; use inputSchema.required")
	}
	mapNode := getMap(t, properties, "map")
	if _, ok := mapNode["required"]; ok {
		t.Fatalf("map property must not define scalar required; use inputSchema.required")
	}
}

func containsString(values []any, want string) bool {
	for _, raw := range values {
		if text, ok := raw.(string); ok && text == want {
			return true
		}
	}
	return false
}
