package panel

import (
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
)

func TestClashPresetStructure(t *testing.T) {
	proxies := []map[string]any{{"name": "tokyo", "type": "ss", "server": "1.1.1.1", "port": 1}}
	data, err := marshalClashPreset(proxies, []clashPresetMember{{Name: "tokyo"}})
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

func TestClashPresetAssignsNodesToSelectedGroups(t *testing.T) {
	proxies := []map[string]any{
		{"name": "ai-only", "type": "ss", "server": "1.1.1.1", "port": 1},
		{"name": "everywhere", "type": "ss", "server": "1.0.0.1", "port": 1},
	}
	data, err := marshalClashPreset(proxies, []clashPresetMember{
		{Name: "ai-only", Groups: []string{"ai"}},
		{Name: "everywhere"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Proxies []struct {
			Name string `yaml:"name"`
		} `yaml:"proxies"`
		Groups []struct {
			Name    string   `yaml:"name"`
			Type    string   `yaml:"type"`
			Proxies []string `yaml:"proxies"`
		} `yaml:"proxy-groups"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Proxies) != 2 {
		t.Fatalf("proxies = %d, want both nodes in the proxy list", len(doc.Proxies))
	}
	byName := map[string][]string{}
	for _, group := range doc.Groups {
		byName[group.Name] = group.Proxies
		if group.Type == "url-test" || group.Type == "fallback" {
			for _, proxy := range group.Proxies {
				if proxy == "ai-only" {
					t.Errorf("%s includes ai-only", group.Name)
				}
			}
		}
	}
	contains := func(group, name string) bool {
		for _, proxy := range byName[group] {
			if proxy == name {
				return true
			}
		}
		return false
	}
	if !contains(grpAI, "ai-only") {
		t.Fatalf("AI group = %v, want ai-only", byName[grpAI])
	}
	for _, group := range []string{grpSelect, grpApple, grpGoogle, grpTelegram, grpFinal} {
		if contains(group, "ai-only") {
			t.Errorf("%s includes ai-only: %v", group, byName[group])
		}
		if !contains(group, "everywhere") {
			t.Errorf("%s missing everywhere: %v", group, byName[group])
		}
	}
	if _, ok := byName[grpAuto]; !ok {
		t.Fatal("url-test group missing while an unrestricted node remains")
	}
}

func TestNormalizePresetGroups(t *testing.T) {
	all, err := normalizePresetGroups([]string{" AI ", "ai", "select", "auto", "fallback", "apple", "google", "telegram", "final"})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 0 {
		t.Fatalf("full selection = %v, want empty (every group)", all)
	}
	got, err := normalizePresetGroups([]string{"telegram", "ai", "ai"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "ai,telegram" {
		t.Fatalf("groups = %v", got)
	}
	if _, err := normalizePresetGroups([]string{"nope"}); err == nil {
		t.Fatal("unknown group accepted")
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
