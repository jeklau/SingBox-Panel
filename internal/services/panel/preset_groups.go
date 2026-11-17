package panel

import (
	"fmt"
	"strings"
)

// presetGroupOrder is the built-in Clash policy groups a node can join.
// Direct and block groups never list nodes.
var presetGroupOrder = []string{
	"select", "auto", "fallback", "apple", "google", "ai", "telegram", "final",
}

// normalizePresetGroups keeps known group keys in a stable order. An empty
// result means the node joins every group. Selecting every current group is
// stored the same way, so a later group still includes that node.
func normalizePresetGroups(groups []string) ([]string, error) {
	allowed := make(map[string]struct{}, len(presetGroupOrder))
	for _, key := range presetGroupOrder {
		allowed[key] = struct{}{}
	}
	seen := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		group = strings.ToLower(strings.TrimSpace(group))
		if group == "" {
			continue
		}
		if _, ok := allowed[group]; !ok {
			return nil, fmt.Errorf("未知的分流规则 %q", group)
		}
		seen[group] = struct{}{}
	}
	if len(seen) == 0 || len(seen) == len(presetGroupOrder) {
		return []string{}, nil
	}
	out := make([]string, 0, len(seen))
	for _, key := range presetGroupOrder {
		if _, ok := seen[key]; ok {
			out = append(out, key)
		}
	}
	return out, nil
}

// presetGroupIncludes reports whether a node belongs in one policy group.
// An empty selection is the historical "every group" behaviour.
func presetGroupIncludes(groups []string, key string) bool {
	if len(groups) == 0 {
		return true
	}
	for _, group := range groups {
		if group == key {
			return true
		}
	}
	return false
}
