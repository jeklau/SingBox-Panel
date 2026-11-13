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

// clashPresetGroups returns the ten policy groups over the given node names.
func clashPresetGroups(names []string) []yaml.MapSlice {
	withNodes := func(head ...string) []string {
		out := append([]string{}, head...)
		return append(out, names...)
	}
	group := func(name, typ string, proxies []string, extra ...yaml.MapItem) yaml.MapSlice {
		g := yaml.MapSlice{{Key: "name", Value: name}, {Key: "type", Value: typ}}
		g = append(g, extra...)
		return append(g, yaml.MapItem{Key: "proxies", Value: proxies})
	}
	probe := func() []yaml.MapItem {
		return []yaml.MapItem{{Key: "url", Value: presetProbeURL}, {Key: "interval", Value: 300}}
	}
	return []yaml.MapSlice{
		group(grpSelect, "select", withNodes(grpAuto, grpFallback, "DIRECT")),
		group(grpAuto, "url-test", names, append(probe(), yaml.MapItem{Key: "tolerance", Value: 50})...),
		group(grpFallback, "fallback", names, probe()...),
		group(grpDirect, "select", []string{"DIRECT", grpSelect}),
		group(grpApple, "select", withNodes("DIRECT", grpSelect, grpAuto)),
		group(grpGoogle, "select", withNodes(grpSelect, grpAuto)),
		group(grpAI, "select", withNodes(grpSelect, grpAuto)),
		group(grpTelegram, "select", withNodes(grpSelect, grpAuto)),
		group(grpBlock, "select", []string{"REJECT", "DIRECT"}),
		group(grpFinal, "select", withNodes(grpSelect, "DIRECT", grpAuto)),
	}
}

// marshalClashPreset renders a complete mihomo config. Callers must pass at
// least one proxy: url-test and fallback groups are invalid when empty.
func marshalClashPreset(proxies []map[string]any, names []string) ([]byte, error) {
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
		{Key: "proxy-groups", Value: clashPresetGroups(names)},
		{Key: "rules", Value: builtinClashRules},
	}
	return yaml.MarshalWithOptions(doc, yaml.IndentSequence(true))
}
