package panel

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/hann0w0/singbox-panel/internal/domain/model"
)

// managedTemplateNode is the single definition of the metadata a template sees
// for a panel-managed inbound. Subscription building and sing-box user
// provisioning both match through it, so a user is never handed a node whose
// credentials the server does not know. The name is deliberately the stable
// "<server> <tag>" pair, not the display name, which depends on sibling nodes.
func managedTemplateNode(srv *model.Server, ib *model.Inbound) model.TemplateNode {
	return model.TemplateNode{
		Name:     srv.Name + " " + ib.Tag,
		Tag:      ib.Tag,
		Protocol: string(ib.Type),
		Server:   srv.Name,
		Region:   srv.Region,
		Source:   "managed",
	}
}

// enabledTemplates loads every enabled template keyed by id.
func enabledTemplates(db *gorm.DB) (map[uint]*model.SubscriptionTemplate, error) {
	var rows []model.SubscriptionTemplate
	if err := db.Where("enabled = ?", true).Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[uint]*model.SubscriptionTemplate, len(rows))
	for i := range rows {
		out[rows[i].ID] = &rows[i]
	}
	return out, nil
}

// userInboundGranted reports whether the user may use the inbound through
// explicit access or through the template bound to them (tpl may be nil).
func userInboundGranted(user *model.User, tpl *model.SubscriptionTemplate, srv *model.Server, ib *model.Inbound) bool {
	if user.HasInbound(srv.ID, ib.ID) {
		return true
	}
	return tpl != nil && tpl.Matches(managedTemplateNode(srv, ib))
}

func (a *App) allServerIDs() []uint {
	var ids []uint
	if err := a.db.Model(&model.Server{}).Pluck("id", &ids).Error; err != nil {
		return nil
	}
	return ids
}

type subscriptionTemplateReq struct {
	Name       string               `json:"name"`
	Enabled    *bool                `json:"enabled"`
	IncludeAll bool                 `json:"include_all"`
	Rules      []model.TemplateRule `json:"rules"`
}

type subscriptionTemplateView struct {
	model.SubscriptionTemplate
	UserCount int64 `json:"user_count"`
}

func (a *App) listSubscriptionTemplates(c *gin.Context) {
	var rows []model.SubscriptionTemplate
	if err := a.db.Order("id").Find(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	counts := map[uint]int64{}
	var grouped []struct {
		TemplateID uint
		N          int64
	}
	if err := a.db.Model(&model.User{}).Select("template_id AS template_id, COUNT(*) AS n").
		Where("template_id IS NOT NULL").Group("template_id").Scan(&grouped).Error; err == nil {
		for _, g := range grouped {
			counts[g.TemplateID] = g.N
		}
	}
	views := make([]subscriptionTemplateView, 0, len(rows))
	for _, r := range rows {
		if r.Rules == nil {
			r.Rules = []model.TemplateRule{}
		}
		views = append(views, subscriptionTemplateView{SubscriptionTemplate: r, UserCount: counts[r.ID]})
	}
	c.JSON(http.StatusOK, gin.H{"templates": views})
}

func (a *App) createSubscriptionTemplate(c *gin.Context) {
	var req subscriptionTemplateReq
	if !bindJSON(c, &req) {
		return
	}
	row := model.SubscriptionTemplate{Name: req.Name, Enabled: true, IncludeAll: req.IncludeAll, Rules: req.Rules}
	if req.Enabled != nil {
		row.Enabled = *req.Enabled
	}
	if err := row.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	enabled := row.Enabled
	if err := a.db.Create(&row).Error; err != nil {
		if duplicateDatabaseError(err) {
			c.JSON(http.StatusConflict, gin.H{"error": "template name already exists"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Restore an explicit false in case the column carries a default.
	if !enabled {
		if err := a.db.Model(&row).UpdateColumn("enabled", false).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		row.Enabled = false
	}
	c.JSON(http.StatusOK, gin.H{"template": row})
}

func (a *App) updateSubscriptionTemplate(c *gin.Context) {
	id, ok := uintParam(c, "id")
	if !ok {
		return
	}
	var req subscriptionTemplateReq
	if !bindJSON(c, &req) {
		return
	}
	var row model.SubscriptionTemplate
	if err := a.db.First(&row, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "template not found"})
		return
	}
	row.Name, row.IncludeAll, row.Rules = req.Name, req.IncludeAll, req.Rules
	if req.Enabled != nil {
		row.Enabled = *req.Enabled
	}
	if err := row.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := a.db.Model(&row).Select("Name", "Enabled", "IncludeAll", "Rules").Updates(&row).Error; err != nil {
		if duplicateDatabaseError(err) {
			c.JSON(http.StatusConflict, gin.H{"error": "template name already exists"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	a.refreshTemplateUsers(row.ID)
	c.JSON(http.StatusOK, gin.H{"template": row})
}

func (a *App) deleteSubscriptionTemplate(c *gin.Context) {
	id, ok := uintParam(c, "id")
	if !ok {
		return
	}
	var bound int64
	if err := a.db.Model(&model.User{}).Where("template_id = ?", id).Count(&bound).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err := a.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.User{}).Where("template_id = ?", id).UpdateColumn("template_id", nil).Error; err != nil {
			return err
		}
		return tx.Delete(&model.SubscriptionTemplate{}, id).Error
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if bound > 0 {
		a.refreshUserProxyAccess(a.allServerIDs())
	}
	c.Status(http.StatusNoContent)
}

// refreshTemplateUsers re-pushes every managed server when anyone is bound to the
// template, because its matches decide which users appear in each inbound.
func (a *App) refreshTemplateUsers(templateID uint) {
	var bound int64
	if err := a.db.Model(&model.User{}).Where("template_id = ?", templateID).Count(&bound).Error; err != nil || bound == 0 {
		return
	}
	a.refreshUserProxyAccess(a.allServerIDs())
}
