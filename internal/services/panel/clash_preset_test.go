package panel

import (
	"testing"

	"github.com/goccy/go-yaml"
)

func TestClashPresetStructure(t *testing.T) {
	proxies := []map[string]any{{"name": "tokyo", "type": "ss", "server": "1.1.1.1", "port": 1}}
	data, err := marshalClashPreset(proxies, []string{"tokyo"})
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Groups []struct {
			Name    string   `yaml:"name"`
			Type    string   `yaml:"type"`
			Proxies []string `yaml:"proxies"`
		} `yaml:"proxy-groups"`
		Rules []string `yaml:"rules"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Groups) != 10 {
		t.Fatalf("groups = %d, want 10", len(doc.Groups))
	}
	known := map[string]bool{"DIRECT": true, "REJECT": true, "tokyo": true}
	for _, g := range doc.Groups {
		known[g.Name] = true
	}
	for _, g := range doc.Groups {
		if len(g.Proxies) == 0 {
			t.Errorf("group %q is empty", g.Name)
		}
		for _, p := range g.Proxies {
			if !known[p] {
				t.Errorf("group %q references unknown %q", g.Name, p)
			}
		}
	}
	if last := doc.Rules[len(doc.Rules)-1]; last != "MATCH,"+grpFinal {
		t.Errorf("last rule = %q", last)
	}
	for _, r := range doc.Rules {
		parts := splitRule(r)
		target := parts[len(parts)-1]
		if target == "no-resolve" {
			target = parts[len(parts)-2]
		}
		if !known[target] {
			t.Errorf("rule %q targets unknown group %q", r, target)
		}
	}
}

func splitRule(r string) []string {
	var out []string
	start := 0
	for i := 0; i < len(r); i++ {
		if r[i] == ',' {
			out = append(out, r[start:i])
			start = i + 1
		}
	}
	return append(out, r[start:])
}
