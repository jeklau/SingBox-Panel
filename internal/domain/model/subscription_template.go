package model

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Template rule fields, match modes and node sources.
const (
	TemplateFieldName     = "name"
	TemplateFieldTag      = "tag"
	TemplateFieldProtocol = "protocol"
	TemplateFieldServer   = "server"
	TemplateFieldRegion   = "region"
	TemplateFieldGroup    = "group"
	TemplateFieldSource   = "source" // "managed" (panel inbound) or "external" (custom node)

	TemplateMatchContains = "contains"
	TemplateMatchEquals   = "equals"
	TemplateMatchRegex    = "regex"

	maxTemplateRules      = 64
	maxTemplateValueBytes = 256
)

// SubscriptionTemplate selects nodes for a user automatically. A user bound to a
// template receives every node they are explicitly granted plus every node the
// template matches, so a node added later shows up without editing each user.
//
// Matching: with IncludeAll every node matches; otherwise a node must satisfy at
// least one include rule. Any exclude rule that matches then removes the node.
type SubscriptionTemplate struct {
	ID         uint           `gorm:"primaryKey" json:"id"`
	Name       string         `gorm:"size:128;uniqueIndex" json:"name"`
	Enabled    bool           `gorm:"index" json:"enabled"`
	IncludeAll bool           `json:"include_all"`
	Rules      []TemplateRule `gorm:"serializer:json" json:"rules"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// TemplateRule is one include or exclude condition.
type TemplateRule struct {
	Field   string `json:"field"`
	Match   string `json:"match"`
	Value   string `json:"value"`
	Exclude bool   `json:"exclude"`
}

// TemplateNode is the node metadata a template can filter on.
type TemplateNode struct {
	Name     string
	Tag      string
	Protocol string
	Server   string
	Region   string
	Group    string
	Source   string
}

func (n TemplateNode) field(f string) string {
	switch f {
	case TemplateFieldName:
		return n.Name
	case TemplateFieldTag:
		return n.Tag
	case TemplateFieldProtocol:
		return n.Protocol
	case TemplateFieldServer:
		return n.Server
	case TemplateFieldRegion:
		return n.Region
	case TemplateFieldGroup:
		return n.Group
	case TemplateFieldSource:
		return n.Source
	}
	return ""
}

// Validate normalizes the template in place and reports the first problem.
func (t *SubscriptionTemplate) Validate() error {
	t.Name = strings.TrimSpace(t.Name)
	if t.Name == "" {
		return fmt.Errorf("template name is required")
	}
	if len(t.Name) > 128 {
		return fmt.Errorf("template name is too long")
	}
	if len(t.Rules) > maxTemplateRules {
		return fmt.Errorf("at most %d rules are allowed", maxTemplateRules)
	}
	for i := range t.Rules {
		r := &t.Rules[i]
		r.Field = strings.ToLower(strings.TrimSpace(r.Field))
		r.Match = strings.ToLower(strings.TrimSpace(r.Match))
		if r.Match == "" {
			r.Match = TemplateMatchContains
		}
		if !validTemplateField(r.Field) {
			return fmt.Errorf("rule %d: unknown field %q", i+1, r.Field)
		}
		if strings.TrimSpace(r.Value) == "" {
			return fmt.Errorf("rule %d: value is required", i+1)
		}
		if len(r.Value) > maxTemplateValueBytes {
			return fmt.Errorf("rule %d: value is too long", i+1)
		}
		switch r.Match {
		case TemplateMatchContains, TemplateMatchEquals:
		case TemplateMatchRegex:
			if _, err := regexp.Compile(r.Value); err != nil {
				return fmt.Errorf("rule %d: invalid regular expression: %v", i+1, err)
			}
		default:
			return fmt.Errorf("rule %d: unknown match mode %q", i+1, r.Match)
		}
	}
	return nil
}

func validTemplateField(f string) bool {
	switch f {
	case TemplateFieldName, TemplateFieldTag, TemplateFieldProtocol, TemplateFieldServer,
		TemplateFieldRegion, TemplateFieldGroup, TemplateFieldSource:
		return true
	}
	return false
}

// Matches reports whether the template selects the node. A disabled template
// matches nothing.
func (t *SubscriptionTemplate) Matches(n TemplateNode) bool {
	if t == nil || !t.Enabled {
		return false
	}
	selected := t.IncludeAll
	for _, r := range t.Rules {
		if r.Exclude || !r.matches(n) {
			continue
		}
		selected = true
		break
	}
	if !selected {
		return false
	}
	for _, r := range t.Rules {
		if r.Exclude && r.matches(n) {
			return false
		}
	}
	return true
}

func (r TemplateRule) matches(n TemplateNode) bool {
	got := n.field(r.Field)
	switch r.Match {
	case TemplateMatchEquals:
		return strings.EqualFold(got, r.Value)
	case TemplateMatchRegex:
		re, err := regexp.Compile(r.Value)
		return err == nil && re.MatchString(got)
	default:
		return strings.Contains(strings.ToLower(got), strings.ToLower(r.Value))
	}
}
