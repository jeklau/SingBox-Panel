package panel

import (
	"testing"

	"github.com/hann0w0/singbox-panel/internal/domain/model"
)

func templateTestFixture(t *testing.T) (a *App, hk, jp model.Server, hkIn, jpIn model.Inbound, user model.User) {
	t.Helper()
	db := testDB(t)
	a = &App{db: db}
	hk = model.Server{Name: "香港", AgentToken: "tpl-agent-hk", Address: "hk.example.com", Region: "HK"}
	jp = model.Server{Name: "东京", AgentToken: "tpl-agent-jp", Address: "jp.example.com", Region: "JP"}
	for _, s := range []*model.Server{&hk, &jp} {
		if err := db.Create(s).Error; err != nil {
			t.Fatal(err)
		}
	}
	mk := func(srv model.Server, port int) model.Inbound {
		ib := model.Inbound{
			ServerID: srv.ID, Tag: "vless-in", Type: model.InboundVLESS, ListenPort: port, Enabled: true,
			Settings: model.JSONText(`{"uuid":"11111111-2222-3333-4444-555555555555"}`),
		}
		if err := db.Create(&ib).Error; err != nil {
			t.Fatal(err)
		}
		return ib
	}
	hkIn, jpIn = mk(hk, 30001), mk(jp, 30002)
	user = model.User{Email: "tpl-user", Role: model.RoleUser, Enabled: true, SubToken: "tpl-sub-token", ServerIDs: []uint{hk.ID}}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	return
}

func bindTemplate(t *testing.T, a *App, user *model.User, tpl model.SubscriptionTemplate) {
	t.Helper()
	if err := tpl.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := a.db.Create(&tpl).Error; err != nil {
		t.Fatal(err)
	}
	// Enabled is stored explicitly so a false value survives column defaults.
	if err := a.db.Model(&tpl).UpdateColumn("enabled", tpl.Enabled).Error; err != nil {
		t.Fatal(err)
	}
	user.TemplateID = &tpl.ID
	if err := a.db.Model(user).UpdateColumn("template_id", tpl.ID).Error; err != nil {
		t.Fatal(err)
	}
}

func nodeServers(nodes []node) map[string]bool {
	out := map[string]bool{}
	for _, n := range nodes {
		out[n.server] = true
	}
	return out
}

func TestTemplateAddsMatchingNodesToExplicitAccess(t *testing.T) {
	a, _, _, _, _, user := templateTestFixture(t)

	nodes, err := a.gatherNodesWithError(&user)
	if err != nil {
		t.Fatal(err)
	}
	if got := nodeServers(nodes); len(got) != 1 || !got["hk.example.com"] {
		t.Fatalf("without a template want only the granted server, got %v", got)
	}

	bindTemplate(t, a, &user, model.SubscriptionTemplate{
		Name: "jp", Enabled: true, Rules: []model.TemplateRule{{Field: "region", Match: "equals", Value: "JP"}},
	})
	nodes, err = a.gatherNodesWithError(&user)
	if err != nil {
		t.Fatal(err)
	}
	if got := nodeServers(nodes); len(got) != 2 || !got["hk.example.com"] || !got["jp.example.com"] {
		t.Fatalf("template should add the JP node to the explicit HK node, got %v", got)
	}
}

func TestTemplateCoversNodesAddedLater(t *testing.T) {
	a, _, jp, _, _, user := templateTestFixture(t)
	bindTemplate(t, a, &user, model.SubscriptionTemplate{Name: "all", Enabled: true, IncludeAll: true})

	later := model.Inbound{
		ServerID: jp.ID, Tag: "vless-new", Type: model.InboundVLESS, ListenPort: 30003, Enabled: true,
		Settings: model.JSONText(`{"uuid":"99999999-2222-3333-4444-555555555555"}`),
	}
	if err := a.db.Create(&later).Error; err != nil {
		t.Fatal(err)
	}
	nodes, err := a.gatherNodesWithError(&user)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range nodes {
		if n.tag == "vless-new" {
			found = true
		}
	}
	if !found {
		t.Fatalf("include-all template must pick up an inbound created afterwards; tags: %v", nodes)
	}
}

func TestDisabledTemplateAddsNothing(t *testing.T) {
	a, _, _, _, _, user := templateTestFixture(t)
	bindTemplate(t, a, &user, model.SubscriptionTemplate{Name: "off", Enabled: false, IncludeAll: true})
	nodes, err := a.gatherNodesWithError(&user)
	if err != nil {
		t.Fatal(err)
	}
	if got := nodeServers(nodes); len(got) != 1 || !got["hk.example.com"] {
		t.Fatalf("a disabled template must not widen access, got %v", got)
	}
}

func TestTemplateExcludeDoesNotRevokeExplicitAccess(t *testing.T) {
	a, _, _, _, _, user := templateTestFixture(t)
	bindTemplate(t, a, &user, model.SubscriptionTemplate{
		Name: "no-hk", Enabled: true, IncludeAll: true,
		Rules: []model.TemplateRule{{Field: "region", Match: "equals", Value: "HK", Exclude: true}},
	})
	nodes, err := a.gatherNodesWithError(&user)
	if err != nil {
		t.Fatal(err)
	}
	got := nodeServers(nodes)
	if !got["hk.example.com"] || !got["jp.example.com"] {
		t.Fatalf("explicit HK access must survive the template's exclude rule, got %v", got)
	}
}

// The subscription and the sing-box user list must agree: a node handed out via
// a template is only usable if the server also knows the user.
func TestUserInboundGrantedMatchesTemplate(t *testing.T) {
	a, hk, jp, hkIn, jpIn, user := templateTestFixture(t)
	_ = a
	tpl := &model.SubscriptionTemplate{Enabled: true, Rules: []model.TemplateRule{{Field: "region", Match: "equals", Value: "JP"}}}

	if !userInboundGranted(&user, nil, &hk, &hkIn) {
		t.Fatal("explicit access must grant the HK inbound")
	}
	if userInboundGranted(&user, nil, &jp, &jpIn) {
		t.Fatal("no template and no explicit access must not grant the JP inbound")
	}
	if !userInboundGranted(&user, tpl, &jp, &jpIn) {
		t.Fatal("the template must grant the JP inbound")
	}
}
