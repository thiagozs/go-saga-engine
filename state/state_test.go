package state

import (
	"reflect"
	"testing"
)

type clonePayloadItem struct {
	Name string
	Tags []string
}

func TestClonePayloadDeepCopiesNestedValues(t *testing.T) {
	t.Parallel()

	item := &clonePayloadItem{Name: "a", Tags: []string{"x"}}
	var nilMap map[string]any
	payload := map[string]any{
		"map":   map[string]any{"k": []any{1, "two"}},
		"slice": []string{"a", "b"},
		"array": [2]int{1, 2},
		"ptr":   item,
		"nil":   nil,
		"nilm":  nilMap,
		"int":   7,
	}
	cloned := ClonePayload(payload)
	if !reflect.DeepEqual(payload, cloned) {
		t.Fatalf("expected equal clone, got %#v", cloned)
	}

	cloned["map"].(map[string]any)["k"].([]any)[0] = 99
	cloned["slice"].([]string)[0] = "z"
	cloned["ptr"].(*clonePayloadItem).Tags[0] = "changed"
	if payload["map"].(map[string]any)["k"].([]any)[0] != 1 ||
		payload["slice"].([]string)[0] != "a" || item.Tags[0] != "x" {
		t.Fatal("clone shares memory with original")
	}
}

func TestClonePayloadNilReturnsEmptyMap(t *testing.T) {
	t.Parallel()

	if got := ClonePayload(nil); got == nil || len(got) != 0 {
		t.Fatalf("expected empty map, got %#v", got)
	}
	if CloneValue(nil) != nil {
		t.Fatal("expected nil")
	}
}
