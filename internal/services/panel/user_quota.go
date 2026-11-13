package panel

import (
	"time"

	"gorm.io/gorm"

	"github.com/hann0w0/singbox-panel/internal/domain/model"
)

// quotaUsage is one user's attributed traffic in the current monthly cycle.
type quotaUsage struct {
	Upload    uint64
	Download  uint64
	Start     time.Time
	NextReset time.Time
}

func (q quotaUsage) total() uint64 { return q.Upload + q.Download }

// userCycleUsage sums the user's attributed traffic since the most recent UTC
// reset. Only multi-user inbounds attribute bytes to a user.
func userCycleUsage(db *gorm.DB, u *model.User, now time.Time) (quotaUsage, error) {
	start, next := trafficCycleBounds(now, u.TrafficResetDay)
	usage := quotaUsage{Start: start, NextReset: next}
	var row struct {
		Upload   uint64
		Download uint64
	}
	if err := db.Model(&model.TrafficUserHourly{}).
		Select("COALESCE(SUM(upload), 0) AS upload, COALESCE(SUM(download), 0) AS download").
		Where("user_id = ? AND bucket >= ? AND bucket < ?", u.ID, start.UTC().Truncate(time.Hour), next.UTC()).
		Scan(&row).Error; err != nil {
		return usage, err
	}
	usage.Upload, usage.Download = row.Upload, row.Download
	return usage, nil
}

func quotaExceeded(u *model.User, usage quotaUsage) bool {
	return u.TrafficQuotaBytes > 0 && usage.total() >= u.TrafficQuotaBytes
}

// userOverQuota reports whether the user has exhausted this cycle's quota. A
// user without a quota never queries the ledger.
func userOverQuota(db *gorm.DB, u *model.User, now time.Time) (bool, error) {
	if u.TrafficQuotaBytes == 0 {
		return false, nil
	}
	usage, err := userCycleUsage(db, u, now)
	if err != nil {
		return false, err
	}
	return quotaExceeded(u, usage), nil
}

// userTrafficInfo is the usage summary returned to a user and shown to admins.
type userTrafficInfo struct {
	Used       uint64    `json:"used"`
	Upload     uint64    `json:"upload"`
	Download   uint64    `json:"download"`
	Quota      uint64    `json:"quota"`
	CycleStart time.Time `json:"cycle_start"`
	NextReset  time.Time `json:"next_reset"`
	Exceeded   bool      `json:"exceeded"`
}

func newUserTrafficInfo(u *model.User, usage quotaUsage) userTrafficInfo {
	return userTrafficInfo{
		Used: usage.total(), Upload: usage.Upload, Download: usage.Download, Quota: u.TrafficQuotaBytes,
		CycleStart: usage.Start, NextReset: usage.NextReset, Exceeded: quotaExceeded(u, usage),
	}
}
