package engine

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/thiagozs/go-saga-engine/state"
)

type fuzzStage string

func (s fuzzStage) Name() string                                   { return string(s) }
func (s fuzzStage) Execute(context.Context, *state.State) error    { return nil }
func (s fuzzStage) Compensate(context.Context, *state.State) error { return nil }

func FuzzValidateAndSort(f *testing.F) {
	f.Add([]byte{3, 0, 1, 1, 1, 2})
	f.Add([]byte{2, 1, 1, 1, 0})
	f.Add([]byte{0})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		count := int(data[0] % 9)
		nodes := make([]Node, count)
		cursor := 1
		for i := 0; i < count; i++ {
			name := fmt.Sprintf("stage-%d", i)
			nodes[i] = Node{Stage: fuzzStage(name), Parallel: i%2 == 0}
			if cursor >= len(data) {
				continue
			}
			dependencyCount := int(data[cursor] % 4)
			cursor++
			for j := 0; j < dependencyCount && cursor < len(data); j++ {
				dependency := int(data[cursor]) % (count + 1)
				cursor++
				if dependency == count {
					nodes[i].DependsOn = append(nodes[i].DependsOn, "unknown")
				} else {
					nodes[i].DependsOn = append(nodes[i].DependsOn, fmt.Sprintf("stage-%d", dependency))
				}
			}
		}

		ordered, _, err := validateAndSort(nodes)
		if err != nil {
			return
		}
		if len(ordered) != len(nodes) {
			t.Fatalf("valid graph lost nodes: got %d, want %d", len(ordered), len(nodes))
		}
		positions := make(map[string]int, len(ordered))
		names := make([]string, 0, len(ordered))
		for i, node := range ordered {
			name := node.Stage.Name()
			positions[name] = i
			names = append(names, name)
		}
		for _, node := range ordered {
			for _, dependency := range node.DependsOn {
				if positions[dependency] >= positions[node.Stage.Name()] {
					t.Fatalf("dependency %q is not before %q", dependency, node.Stage.Name())
				}
			}
		}

		repeated, _, err := validateAndSort(nodes)
		if err != nil {
			t.Fatalf("same graph changed validity: %v", err)
		}
		repeatedNames := make([]string, 0, len(repeated))
		for _, node := range repeated {
			repeatedNames = append(repeatedNames, node.Stage.Name())
		}
		if !slices.Equal(names, repeatedNames) {
			t.Fatalf("sort is unstable: %v != %v", names, repeatedNames)
		}
	})
}
