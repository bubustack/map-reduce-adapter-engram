package main

import (
	"os"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestEngramTemplateRBACMatchesTriggerStoryContract(t *testing.T) {
	t.Helper()

	raw, err := os.ReadFile("Engram.yaml")
	if err != nil {
		t.Fatalf("read Engram.yaml: %v", err)
	}

	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal Engram.yaml: %v", err)
	}

	spec := getMap(t, doc, "spec")
	execution := getMap(t, spec, "execution")
	rbac := getMap(t, execution, "rbac")
	rules := getSlice(t, rbac, "rules")

	if !hasRule(rules, []string{"runs.bubustack.io"}, []string{"storytriggers"}, []string{"create", "get"}) {
		t.Fatal("Engram.yaml must grant storytriggers create/get for TriggerStory")
	}
	if !hasRule(rules, []string{"runs.bubustack.io"}, []string{"storyruns"}, []string{"get"}) {
		t.Fatal("Engram.yaml must grant storyruns get for TriggerStory resolution")
	}
}

func getMap(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()
	val, ok := parent[key]
	if !ok {
		t.Fatalf("missing key %q", key)
	}
	out, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("key %q is not a map", key)
	}
	return out
}

func getSlice(t *testing.T, parent map[string]any, key string) []any {
	t.Helper()
	val, ok := parent[key]
	if !ok {
		t.Fatalf("missing key %q", key)
	}
	out, ok := val.([]any)
	if !ok {
		t.Fatalf("key %q is not a slice", key)
	}
	return out
}

func hasRule(rules []any, groups, resources, verbs []string) bool {
	for _, raw := range rules {
		rule, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if matchStringSlice(rule["apiGroups"], groups) &&
			matchStringSlice(rule["resources"], resources) &&
			matchStringSlice(rule["verbs"], verbs) {
			return true
		}
	}
	return false
}

func matchStringSlice(raw any, want []string) bool {
	got, ok := raw.([]any)
	if !ok || len(got) != len(want) {
		return false
	}
	for i, item := range got {
		s, ok := item.(string)
		if !ok || s != want[i] {
			return false
		}
	}
	return true
}
