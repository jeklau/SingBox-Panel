package model

import "testing"

func TestTemplateMatches(t *testing.T) {
	hk := TemplateNode{Name: "香港 vless-in", Tag: "vless-in", Protocol: "vless", Server: "香港", Region: "HK", Source: "managed"}
	jp := TemplateNode{Name: "东京 hy2-in", Tag: "hy2-in", Protocol: "hysteria2", Server: "东京", Region: "JP", Source: "managed"}
	ext := TemplateNode{Name: "Airport US 01", Protocol: "trojan", Region: "US", Group: "airport", Source: "external"}

	cases := []struct {
		name string
		tpl  SubscriptionTemplate
		node TemplateNode
		want bool
	}{
		{"include all", SubscriptionTemplate{Enabled: true, IncludeAll: true}, hk, true},
		{"disabled matches nothing", SubscriptionTemplate{Enabled: false, IncludeAll: true}, hk, false},
		{"no include rule matches nothing", SubscriptionTemplate{Enabled: true}, hk, false},
		{"contains is case-insensitive", SubscriptionTemplate{Enabled: true, Rules: []TemplateRule{{Field: "protocol", Match: "contains", Value: "VLESS"}}}, hk, true},
		{"equals", SubscriptionTemplate{Enabled: true, Rules: []TemplateRule{{Field: "region", Match: "equals", Value: "jp"}}}, jp, true},
		{"equals rejects partial", SubscriptionTemplate{Enabled: true, Rules: []TemplateRule{{Field: "region", Match: "equals", Value: "J"}}}, jp, false},
		{"regex", SubscriptionTemplate{Enabled: true, Rules: []TemplateRule{{Field: "name", Match: "regex", Value: `^Airport .* \d+$`}}}, ext, true},
		{"any include rule suffices", SubscriptionTemplate{Enabled: true, Rules: []TemplateRule{
			{Field: "region", Match: "equals", Value: "HK"}, {Field: "region", Match: "equals", Value: "JP"},
		}}, jp, true},
		{"exclude beats include", SubscriptionTemplate{Enabled: true, IncludeAll: true, Rules: []TemplateRule{
			{Field: "source", Match: "equals", Value: "external", Exclude: true},
		}}, ext, false},
		{"exclude leaves others", SubscriptionTemplate{Enabled: true, IncludeAll: true, Rules: []TemplateRule{
			{Field: "source", Match: "equals", Value: "external", Exclude: true},
		}}, hk, true},
		{"only exclude rules select nothing", SubscriptionTemplate{Enabled: true, Rules: []TemplateRule{
			{Field: "region", Match: "equals", Value: "US", Exclude: true},
		}}, hk, false},
		{"group field", SubscriptionTemplate{Enabled: true, Rules: []TemplateRule{{Field: "group", Match: "equals", Value: "airport"}}}, ext, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.tpl.Matches(tc.node); got != tc.want {
				t.Fatalf("Matches = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNilTemplateMatchesNothing(t *testing.T) {
	var tpl *SubscriptionTemplate
	if tpl.Matches(TemplateNode{Name: "x"}) {
		t.Fatal("a nil template must match nothing")
	}
}

func TestTemplateValidate(t *testing.T) {
	bad := []SubscriptionTemplate{
		{Name: "  "},
		{Name: "t", Rules: []TemplateRule{{Field: "nope", Value: "x"}}},
		{Name: "t", Rules: []TemplateRule{{Field: "name", Value: " "}}},
		{Name: "t", Rules: []TemplateRule{{Field: "name", Match: "glob", Value: "x"}}},
		{Name: "t", Rules: []TemplateRule{{Field: "name", Match: "regex", Value: "("}}},
	}
	for i := range bad {
		if err := bad[i].Validate(); err == nil {
			t.Errorf("case %d: expected a validation error", i)
		}
	}

	ok := SubscriptionTemplate{Name: " HK ", Rules: []TemplateRule{{Field: " Region ", Match: "", Value: "HK"}}}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	if ok.Name != "HK" || ok.Rules[0].Field != "region" || ok.Rules[0].Match != TemplateMatchContains {
		t.Fatalf("Validate did not normalize: %+v", ok)
	}
}
