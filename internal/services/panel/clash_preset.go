package panel

import (
	"github.com/goccy/go-yaml"
)

// Built-in Clash (mihomo) policy groups and routing rules, served when the
// subscription is fetched with ?target=clash&preset=builtin. The plain
// ?target=clash output stays a bare proxies list for users who keep their own
// rules.
const (
	grpSelect   = "🚀 节点选择"
	grpAuto     = "♻️ 自动选择"
	grpFallback = "🔯 故障转移"
	grpDirect   = "🎯 全球直连"
	grpApple    = "🍎 苹果服务"
	grpGoogle   = "🔍 谷歌服务"
	grpAI       = "🤖 AI 服务"
	grpTelegram = "📲 电报消息"
	grpBlock    = "🛑 全球拦截"
	grpFinal    = "🐟 漏网之鱼"

	presetProbeURL = "https://www.gstatic.com/generate_204"
)

// builtinClashRules are evaluated top to bottom; MATCH must stay last.
var builtinClashRules = []string{
	"GEOSITE,category-ads-all," + grpBlock,
	"GEOSITE,private," + grpDirect,
	"GEOIP,private," + grpDirect + ",no-resolve",
	"GEOSITE,apple," + grpApple,
	"GEOSITE,openai," + grpAI,
	"GEOSITE,anthropic," + grpAI,
	"GEOSITE,category-ai-!cn," + grpAI,
	"GEOSITE,telegram," + grpTelegram,
	"GEOIP,telegram," + grpTelegram + ",no-resolve",
	"GEOSITE,google," + grpGoogle,
	"GEOIP,google," + grpGoogle + ",no-resolve",
	"GEOSITE,cn," + grpDirect,
	"GEOIP,CN," + grpDirect,
	"MATCH," + grpFinal,
}

// clashPresetMember is one subscription proxy and the policy groups it joins.
// An empty Groups list means every group.
type clashPresetMember struct {
	Name   string
	Groups []string
}

// clashPresetGroups returns the ten policy groups. A node is listed only in
// the groups it was assigned to. url-test and fallback are omitted when they
// would have no proxies, because those group types cannot be empty.
func clashPresetGroups(members []clashPresetMember) []yaml.MapSlice {
	names := func(key string) []string {
		out := make([]string, 0, len(members))
		for _, member := range members {
			if presetGroupIncludes(member.Groups, key) {
				out = append(out, member.Name)
			}
		}
		return out
	}
	group := func(name, typ string, proxies []string, extra ...yaml.MapItem) yaml.MapSlice {
		g := yaml.MapSlice{{Key: "name", Value: name}, {Key: "type", Value: typ}}
		g = append(g, extra...)
		return append(g, yaml.MapItem{Key: "proxies", Value: proxies})
	}
	probe := func() []yaml.MapItem {
		return []yaml.MapItem{{Key: "url", Value: presetProbeURL}, {Key: "interval", Value: 300}}
	}
	autoNames := names("auto")
	fallbackNames := names("fallback")
	hasAuto := len(autoNames) > 0
	hasFallback := len(fallbackNames) > 0
	with := func(head []string, key string) []string {
		return append(append([]string{}, head...), names(key)...)
	}
	selectHead := make([]string, 0, 3)
	if hasAuto {
		selectHead = append(selectHead, grpAuto)
	}
	if hasFallback {
		selectHead = append(selectHead, grpFallback)
	}
	selectHead = append(selectHead, "DIRECT")
	appleHead := []string{"DIRECT", grpSelect}
	googleHead := []string{grpSelect}
	finalHead := []string{grpSelect, "DIRECT"}
	if hasAuto {
		appleHead = append(appleHead, grpAuto)
		googleHead = append(googleHead, grpAuto)
		finalHead = append(finalHead, grpAuto)
	}
	groups := []yaml.MapSlice{
		group(grpSelect, "select", with(selectHead, "select")),
	}
	if hasAuto {
		groups = append(groups, group(grpAuto, "url-test", autoNames, append(probe(), yaml.MapItem{Key: "tolerance", Value: 50})...))
	}
	if hasFallback {
		groups = append(groups, group(grpFallback, "fallback", fallbackNames, probe()...))
	}
	groups = append(groups,
		group(grpDirect, "select", []string{"DIRECT", grpSelect}),
		group(grpApple, "select", with(appleHead, "apple")),
		group(grpGoogle, "select", with(googleHead, "google")),
		group(grpAI, "select", with(append([]string{}, googleHead...), "ai")),
		group(grpTelegram, "select", with(append([]string{}, googleHead...), "telegram")),
		group(grpBlock, "select", []string{"REJECT", "DIRECT"}),
		group(grpFinal, "select", with(finalHead, "final")),
	)
	return groups
}

// marshalClashPreset renders a complete mihomo config. Callers must pass at
// least one proxy: a config with no nodes is not a usable preset.
func marshalClashPreset(proxies []map[string]any, members []clashPresetMember) ([]byte, error) {
	ordered := make([]yaml.MapSlice, 0, len(proxies))
	for _, proxy := range proxies {
		ordered = append(ordered, orderedClashProxyMap(proxy))
	}
	doc := yaml.MapSlice{
		{Key: "mixed-port", Value: 7890},
		{Key: "allow-lan", Value: false},
		{Key: "mode", Value: "rule"},
		{Key: "log-level", Value: "info"},
		{Key: "unified-delay", Value: true},
		{Key: "dns", Value: yaml.MapSlice{
			{Key: "enable", Value: true},
			{Key: "enhanced-mode", Value: "fake-ip"},
			{Key: "nameserver", Value: []string{"https://223.5.5.5/dns-query", "https://doh.pub/dns-query"}},
		}},
		{Key: "proxies", Value: ordered},
		{Key: "proxy-groups", Value: clashPresetGroups(members)},
		{Key: "rules", Value: builtinClashRules},
	}
	return yaml.MarshalWithOptions(doc, yaml.IndentSequence(true))
}
