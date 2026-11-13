package panel

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hann0w0/singbox-panel/internal/domain/model"
)

func quotaTestUser(t *testing.T, a *App, quota uint64, resetDay int) model.User {
	t.Helper()
	u := model.User{
		Email: "quota-user", Role: model.RoleUser, Enabled: true, SubToken: "quota-sub-token",
		TrafficQuotaBytes: quota, TrafficResetDay: resetDay,
	}
	if err := a.db.Create(&u).Error; err != nil {
		t.Fatal(err)
	}
	return u
}

func addUserBucket(t *testing.T, a *App, userID uint, at time.Time, up, down uint64) {
	t.Helper()
	row := model.TrafficUserHourly{ServerID: 1, InboundID: 1, UserID: userID, Bucket: at.UTC().Truncate(time.Hour), Upload: up, Download: down}
	if err := a.db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
}

func TestUserCycleUsageCountsOnlyCurrentCycle(t *testing.T) {
	a := &App{db: testDB(t)}
	u := quotaTestUser(t, a, 1000, 15)
	now := time.Date(2026, 5, 20, 12, 30, 0, 0, time.UTC) // cycle: May 15 .. Jun 15

	addUserBucket(t, a, u.ID, time.Date(2026, 5, 14, 23, 0, 0, 0, time.UTC), 500, 500) // previous cycle
	addUserBucket(t, a, u.ID, time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC), 100, 200)  // first hour of the cycle
	addUserBucket(t, a, u.ID, time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC), 30, 70)

	usage, err := userCycleUsage(a.db, &u, now)
	if err != nil {
		t.Fatal(err)
	}
	if usage.Upload != 130 || usage.Download != 270 {
		t.Fatalf("usage = %d up / %d down, want 130 / 270", usage.Upload, usage.Download)
	}
	if !usage.Start.Equal(time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("cycle start = %v", usage.Start)
	}
}

func TestUserOverQuota(t *testing.T) {
	a := &App{db: testDB(t)}
	u := quotaTestUser(t, a, 400, 1)
	now := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)

	addUserBucket(t, a, u.ID, now, 100, 299)
	if over, err := userOverQuota(a.db, &u, now); err != nil || over {
		t.Fatalf("399 of 400 bytes: over=%v err=%v, want under", over, err)
	}
	extra := model.TrafficUserHourly{ServerID: 1, InboundID: 2, UserID: u.ID, Bucket: now.Truncate(time.Hour), Upload: 1}
	if err := a.db.Create(&extra).Error; err != nil {
		t.Fatal(err)
	}
	if over, err := userOverQuota(a.db, &u, now); err != nil || !over {
		t.Fatalf("400 of 400 bytes: over=%v err=%v, want over", over, err)
	}

	// A new cycle starts counting from zero again.
	if over, err := userOverQuota(a.db, &u, time.Date(2026, 6, 1, 0, 30, 0, 0, time.UTC)); err != nil || over {
		t.Fatalf("after reset: over=%v err=%v, want under", over, err)
	}
}

func TestZeroQuotaIsUnlimited(t *testing.T) {
	a := &App{db: testDB(t)}
	u := quotaTestUser(t, a, 0, 1)
	addUserBucket(t, a, u.ID, time.Now(), 1<<40, 1<<40)
	if over, err := userOverQuota(a.db, &u, time.Now()); err != nil || over {
		t.Fatalf("no quota must never be over: over=%v err=%v", over, err)
	}
}

func TestSubscriptionBlockedOverQuotaAndReportsUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	a := &App{db: testDB(t)}
	u := quotaTestUser(t, a, 1000, 1)
	r := gin.New()
	r.GET("/sub/:token", a.handleSubscription)
	get := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/sub/"+u.SubToken, nil))
		return w
	}

	addUserBucket(t, a, u.ID, time.Now(), 100, 200)
	w := get()
	if w.Code != http.StatusOK {
		t.Fatalf("under quota: status %d body %q", w.Code, w.Body.String())
	}
	info := w.Header().Get("Subscription-Userinfo")
	for _, want := range []string{"upload=100", "download=200", "total=1000"} {
		if !strings.Contains(info, want) {
			t.Fatalf("Subscription-Userinfo %q missing %q", info, want)
		}
	}

	// A second inbound in the same hour keeps the row inside the current cycle
	// even when the test runs right after a reset boundary.
	more := model.TrafficUserHourly{ServerID: 1, InboundID: 2, UserID: u.ID, Bucket: time.Now().UTC().Truncate(time.Hour), Upload: 400, Download: 400}
	if err := a.db.Create(&more).Error; err != nil {
		t.Fatal(err)
	}
	w = get()
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "quota") {
		t.Fatalf("over quota: status %d body %q, want 403 quota", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Header().Get("Subscription-Userinfo"), "total=1000") {
		t.Fatalf("over-quota response should still report usage, got %q", w.Header().Get("Subscription-Userinfo"))
	}
}
