package panel

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/goccy/go-yaml"

	"github.com/hann0w0/singbox-panel/internal/domain/model"
)

func TestPresetGroupsPersistAndFilterBuiltinClash(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := testDB(t)
	hub := NewHub(db)
	app := &App{db: db, hub: hub, orch: NewOrchestrator(db, hub), serverOperations: newKeyedMutex[uint]()}
	router := gin.New()
	router.POST("/servers/:id/inbounds", app.createInbound)
	router.PUT("/servers/:id/inbounds/:inboundID", app.updateInbound)
	router.POST("/custom-nodes", app.createCustomNode)
	router.PUT("/custom-nodes/:id", app.updateCustomNode)

	server := model.Server{Name: "managed", Address: "203.0.113.10", AgentToken: "preset-token", ConfigMode: model.ConfigModeManaged}
	if err := db.Create(&server).Error; err != nil {
		t.Fatal(err)
	}
	user := model.User{Email: "preset-user", Enabled: true, SubToken: "preset-sub", ServerIDs: []uint{}}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}

	inboundPath := "/servers/" + strconv.FormatUint(uint64(server.ID), 10) + "/inbounds"
	created := serveJSONRequest(t, router, http.MethodPost, inboundPath, `{
		"type":"socks","tag":"google-only","listen_port":11080,"enabled":true,
		"settings":{"username":"alice","password":"secret"},
		"preset_groups":["google"]
	}`)
	if created.Code != http.StatusOK {
		t.Fatalf("create inbound: %d %s", created.Code, created.Body.String())
	}
	var inbound model.Inbound
	if err := db.Where("server_id = ?", server.ID).First(&inbound).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(inbound.PresetGroups, []string{"google"}) {
		t.Fatalf("inbound groups = %v", inbound.PresetGroups)
	}
	kept := serveJSONRequest(t, router, http.MethodPut, inboundPath+"/"+strconv.FormatUint(uint64(inbound.ID), 10), `{"remark":"kept"}`)
	if kept.Code != http.StatusOK {
		t.Fatalf("omit groups: %d %s", kept.Code, kept.Body.String())
	}
	if err := db.First(&inbound, inbound.ID).Error; err != nil {
		t.Fatal(err)
	}
	if inbound.Remark != "kept" || !reflect.DeepEqual(inbound.PresetGroups, []string{"google"}) {
		t.Fatalf("omitted update changed groups: remark=%q groups=%v", inbound.Remark, inbound.PresetGroups)
	}
	rejected := serveJSONRequest(t, router, http.MethodPut, inboundPath+"/"+strconv.FormatUint(uint64(inbound.ID), 10), `{"preset_groups":["nope"]}`)
	if rejected.Code != http.StatusBadRequest {
		t.Fatalf("unknown group: %d %s", rejected.Code, rejected.Body.String())
	}

	nodeRes := serveJSONRequest(t, router, http.MethodPost, "/custom-nodes", `{
		"name":"ai-only","protocol":"socks","address":"203.0.113.20","port":1080,
		"all_users":true,"enabled":true,"preset_groups":["AI","ai"]
	}`)
	if nodeRes.Code != http.StatusOK {
		t.Fatalf("create node: %d %s", nodeRes.Code, nodeRes.Body.String())
	}
	var restricted model.CustomNode
	if err := db.Where("name = ?", "ai-only").First(&restricted).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restricted.PresetGroups, []string{"ai"}) {
		t.Fatalf("custom node groups = %v", restricted.PresetGroups)
	}
	cleared := serveJSONRequest(t, router, http.MethodPut, "/custom-nodes/"+strconv.FormatUint(uint64(restricted.ID), 10), `{
		"name":"ai-only","protocol":"socks","address":"203.0.113.20","port":1080,
		"all_users":true,"enabled":true,"preset_groups":[]
	}`)
	if cleared.Code != http.StatusOK {
		t.Fatalf("clear groups: %d %s", cleared.Code, cleared.Body.String())
	}
	if err := db.First(&restricted, restricted.ID).Error; err != nil {
		t.Fatal(err)
	}
	if len(restricted.PresetGroups) != 0 {
		t.Fatalf("empty selection = %v, want every group", restricted.PresetGroups)
	}
	restored := serveJSONRequest(t, router, http.MethodPut, "/custom-nodes/"+strconv.FormatUint(uint64(restricted.ID), 10), `{
		"name":"ai-only","protocol":"socks","address":"203.0.113.20","port":1080,
		"all_users":true,"enabled":true,"preset_groups":["ai"]
	}`)
	if restored.Code != http.StatusOK {
		t.Fatalf("restore groups: %d %s", restored.Code, restored.Body.String())
	}

	open := model.CustomNode{
		Name: "everywhere", Protocol: "socks", Address: "203.0.113.21", Port: 1080,
		AllUsers: true, Enabled: true,
	}
	if err := db.Create(&open).Error; err != nil {
		t.Fatal(err)
	}
	user.ServerIDs = []uint{server.ID}
	if err := db.Save(&user).Error; err != nil {
		t.Fatal(err)
	}
	nodes, err := app.gatherNodesWithError(&user)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/sub/preset-sub?target=clash&preset=builtin", nil)
	app.writeClash(ctx, nodes)
	if rec.Code != http.StatusOK {
		t.Fatalf("clash: %d %s", rec.Code, rec.Body.String())
	}
	var doc struct {
		Groups []struct {
			Name    string   `yaml:"name"`
			Proxies []string `yaml:"proxies"`
		} `yaml:"proxy-groups"`
	}
	if err := yaml.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	byName := map[string][]string{}
	for _, group := range doc.Groups {
		byName[group.Name] = group.Proxies
	}
	contains := func(group, name string) bool {
		for _, proxy := range byName[group] {
			if proxy == name {
				return true
			}
		}
		return false
	}
	if !contains(grpAI, "ai-only") || contains(grpSelect, "ai-only") || contains(grpGoogle, "ai-only") {
		t.Fatalf("ai-only placement: select=%v google=%v ai=%v", byName[grpSelect], byName[grpGoogle], byName[grpAI])
	}
	if !contains(grpGoogle, "managed") || contains(grpAI, "managed") || contains(grpSelect, "managed") {
		t.Fatalf("managed inbound placement: select=%v google=%v ai=%v", byName[grpSelect], byName[grpGoogle], byName[grpAI])
	}
	if !contains(grpSelect, "everywhere") || !contains(grpAI, "everywhere") || !contains(grpGoogle, "everywhere") {
		t.Fatalf("unrestricted node missing from a group: %+v", byName)
	}
}
