//! Per-connection ledger: first observer of a byte wins; tombstones ≤4096.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Timelike, Utc};

use crate::protocol::{PortTrafficSnapshot, UserTrafficSnapshot};

use super::TrafficSamplerInner;

/// sing-box keeps its last 1000 closed connections and replays them on every
/// new stream subscription; remember at least that many.
pub(crate) const MAX_TRAFFIC_TOMBSTONES: usize = 4096;

/// Bound the identity-discovery window for anonymous connections.
pub(crate) const MAX_UNASSIGNED_TRAFFIC_HOURS: i64 = 24;

#[derive(Clone, Debug)]
pub(crate) struct LedgerEntry {
    pub inbound: String,
    pub user: String,
    pub guard_outer: bool,
    pub inbound_type: String,
    pub guard_classified: bool,
    pub unclassified_total: [u64; 2],
    pub node_observed: bool,
    pub node_counted: [u64; 2],
    /// Highest observed total; baseline predates first successful node sample.
    pub counted: [u64; 2],
    pub baseline: [u64; 2],
    /// Attributed part already credited to user or excluded after window expiry.
    pub user_counted: [u64; 2],
    pub first_seen: DateTime<Utc>,
    pub unassigned: HashMap<i64, [u64; 2]>,
}

impl LedgerEntry {
    pub fn new(first_seen: DateTime<Utc>) -> Self {
        Self {
            inbound: String::new(),
            user: String::new(),
            guard_outer: false,
            inbound_type: String::new(),
            guard_classified: false,
            unclassified_total: [0, 0],
            node_observed: false,
            node_counted: [0, 0],
            counted: [0, 0],
            baseline: [0, 0],
            user_counted: [0, 0],
            first_seen,
            unassigned: HashMap::new(),
        }
    }

    pub fn prune_unassigned(&mut self, now: DateTime<Utc>) {
        let cutoff = truncate_hour(now)
            - Duration::hours(MAX_UNASSIGNED_TRAFFIC_HOURS - 1);
        let cutoff_unix = cutoff.timestamp();
        let mut discard_hours: Vec<i64> = self
            .unassigned
            .keys()
            .copied()
            .filter(|h| *h < cutoff_unix)
            .collect();
        for hour in discard_hours.drain(..) {
            if let Some(value) = self.unassigned.remove(&hour) {
                self.user_counted[0] += value[0];
                self.user_counted[1] += value[1];
            }
        }
        while self.unassigned.len() > MAX_UNASSIGNED_TRAFFIC_HOURS as usize {
            let oldest = self.unassigned.keys().copied().min().unwrap();
            if let Some(value) = self.unassigned.remove(&oldest) {
                self.user_counted[0] += value[0];
                self.user_counted[1] += value[1];
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct UserTrafficKey {
    pub inbound: String,
    pub user: String,
}

fn truncate_hour(t: DateTime<Utc>) -> DateTime<Utc> {
    t.with_minute(0)
        .and_then(|t| t.with_second(0))
        .and_then(|t| t.with_nanosecond(0))
        .unwrap_or(t)
}

impl TrafficSamplerInner {
    /// Records a cumulative total for one connection (tests / stream updates).
    pub(crate) fn observe_connection(
        &mut self,
        id: &str,
        inbound: &str,
        user: &str,
        total: [u64; 2],
        baseline: bool,
        now: DateTime<Utc>,
    ) {
        self.observe_connection_from(
            id,
            inbound,
            "",
            user,
            total,
            baseline,
            DateTime::<Utc>::UNIX_EPOCH,
            now,
            None,
        );
    }

    pub(crate) fn observe_connection_from(
        &mut self,
        id: &str,
        inbound: &str,
        inbound_type: &str,
        user: &str,
        mut total: [u64; 2],
        baseline: bool,
        created_at: DateTime<Utc>,
        now: DateTime<Utc>,
        guards: Option<&HashMap<String, bool>>,
    ) {
        let mut from_tombstone = false;
        let mut entry = if let Some(e) = self.ledger.remove(id) {
            e
        } else if let Some(e) = self.tombstones.get(id).cloned() {
            from_tombstone = true;
            e
        } else {
            let mut e = LedgerEntry::new(now);
            if baseline {
                e.counted = total;
                e.baseline = total;
            }
            e
        };

        if entry.inbound.is_empty() {
            entry.inbound = inbound.to_string();
        }
        if entry.inbound_type.is_empty() {
            entry.inbound_type = inbound_type.to_string();
        }
        if !entry.guard_classified
            && !entry.inbound.is_empty()
            && !entry.inbound_type.is_empty()
            && (entry.inbound_type != "direct" || guards.is_some())
        {
            entry.guard_outer = entry.inbound_type == "direct"
                && guards
                    .map(|g| *g.get(&entry.inbound).unwrap_or(&false))
                    .unwrap_or(false);
            entry.guard_classified = true;
        }
        if entry.user.is_empty() {
            entry.user = user.to_string();
        }
        for i in 0..2 {
            total[i] = total[i].max(entry.unclassified_total[i]);
        }
        if entry.inbound_type == "direct" && !entry.guard_classified && self.have_sample {
            entry.unclassified_total = total;
            self.store_ledger_entry(id, entry, from_tombstone);
            return;
        }
        entry.unclassified_total = [0, 0];
        let mut delta = [0u64; 2];
        for i in 0..2 {
            if total[i] > entry.counted[i] {
                delta[i] = total[i] - entry.counted[i];
                entry.counted[i] = total[i];
                if !self.have_sample {
                    entry.baseline[i] += delta[i];
                }
            }
        }
        if !entry.inbound.is_empty() {
            self.observe_node_traffic(&mut entry, created_at, now);
        }
        if !self.have_sample {
            self.store_ledger_entry(id, entry, from_tombstone);
            return;
        }
        if entry.inbound.is_empty() || entry.guard_outer {
            self.store_ledger_entry(id, entry, from_tombstone);
            return;
        }
        if delta[0] > 0 || delta[1] > 0 {
            let window = self.port_window.entry(entry.inbound.clone()).or_insert([0, 0]);
            window[0] += delta[0];
            window[1] += delta[1];
            let port = self
                .pending_ports
                .entry(entry.inbound.clone())
                .or_insert_with(|| PortTrafficSnapshot {
                    inbound: entry.inbound.clone(),
                    upload: 0,
                    download: 0,
                    upload_rate: 0,
                    download_rate: 0,
                });
            port.inbound = entry.inbound.clone();
            port.upload += delta[0];
            port.download += delta[1];
            self.add_port_bucket(now, &entry.inbound, delta);
        }
        if entry.user.is_empty() {
            if delta[0] != 0 || delta[1] != 0 {
                let key = truncate_hour(now).timestamp();
                let v = entry.unassigned.entry(key).or_insert([0, 0]);
                v[0] += delta[0];
                v[1] += delta[1];
            }
            entry.prune_unassigned(now);
            self.store_ledger_entry(id, entry, from_tombstone);
            return;
        }
        entry.prune_unassigned(now);
        let mut user_delta = [0u64; 2];
        for i in 0..2 {
            let attributed = entry.counted[i].saturating_sub(entry.baseline[i]);
            if attributed > entry.user_counted[i] {
                user_delta[i] = attributed - entry.user_counted[i];
                entry.user_counted[i] = attributed;
            }
        }
        if user_delta[0] == 0 && user_delta[1] == 0 {
            self.store_ledger_entry(id, entry, from_tombstone);
            return;
        }
        let key = UserTrafficKey {
            inbound: entry.inbound.clone(),
            user: entry.user.clone(),
        };
        let pending = self
            .pending_users
            .entry(key.clone())
            .or_insert_with(|| UserTrafficSnapshot {
                inbound: entry.inbound.clone(),
                user: entry.user.clone(),
                upload: 0,
                download: 0,
            });
        pending.inbound = entry.inbound.clone();
        pending.user = entry.user.clone();
        pending.upload += user_delta[0];
        pending.download += user_delta[1];

        let mut remaining = user_delta;
        let unassigned: Vec<(i64, [u64; 2])> = entry.unassigned.drain().collect();
        for (bucket, value) in unassigned {
            let credit = [value[0].min(remaining[0]), value[1].min(remaining[1])];
            self.add_user_bucket(
                DateTime::from_timestamp(bucket, 0).unwrap_or(now),
                &key,
                credit,
            );
            remaining[0] -= credit[0];
            remaining[1] -= credit[1];
        }
        if remaining[0] != 0 || remaining[1] != 0 {
            self.add_user_bucket(now, &key, remaining);
        }
        self.store_ledger_entry(id, entry, from_tombstone);
    }

    fn store_ledger_entry(&mut self, id: &str, entry: LedgerEntry, from_tombstone: bool) {
        // Go keeps tombstone entries only in the tombstone map (shared pointer).
        if from_tombstone {
            self.tombstones.insert(id.to_string(), entry);
        } else {
            self.ledger.insert(id.to_string(), entry);
        }
    }

    /// Moves an active entry to the bounded tombstone set.
    pub(crate) fn close_connection(&mut self, id: &str) {
        let Some(entry) = self.ledger.remove(id) else {
            return;
        };
        if !self.tombstones.contains_key(id) {
            self.tombstone_order.push(id.to_string());
        }
        self.tombstones.insert(id.to_string(), entry);
        while self.tombstone_order.len() > MAX_TRAFFIC_TOMBSTONES {
            let old = self.tombstone_order.remove(0);
            self.tombstones.remove(&old);
        }
    }

    /// Closes active entries absent from a complete view taken at `view_started_at`.
    pub(crate) fn close_missing(
        &mut self,
        active: &HashMap<String, bool>,
        view_started_at: DateTime<Utc>,
    ) {
        let to_close: Vec<String> = self
            .ledger
            .iter()
            .filter_map(|(id, entry)| {
                if !active.get(id.as_str()).copied().unwrap_or(false)
                    && entry.first_seen < view_started_at
                {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        for id in to_close {
            self.close_connection(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traffic::{ClashTrafficResponse, TrafficSampler};

    #[test]
    fn unknown_user_attribution_keeps_only_24_utc_hours() {
        let sampler = TrafficSampler::new();
        let start = DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        {
            let mut s = sampler.inner.lock().unwrap();
            s.ingest(
                "api",
                &ClashTrafficResponse {
                    upload_total: 0,
                    download_total: 0,
                    connections: vec![],
                },
                start - Duration::minutes(1),
            );
            let mut total = [0u64; 2];
            for hour in 0..48 {
                for minute in [0, 30] {
                    total[0] += 10;
                    total[1] += 20;
                    s.observe_connection(
                        "long",
                        "in",
                        "",
                        total,
                        false,
                        start
                            + Duration::hours(hour)
                            + Duration::minutes(minute),
                    );
                }
                let n = s.ledger["long"].unassigned.len();
                assert!(
                    n <= MAX_UNASSIGNED_TRAFFIC_HOURS as usize,
                    "deferred identity history grew to {n} hours"
                );
            }
            let entry = s.ledger.get("long").unwrap();
            assert_eq!(entry.unassigned.len(), 24);
            assert_eq!(entry.user_counted, [480, 960]);
            s.close_connection("long");
            assert_eq!(s.tombstones["long"].unassigned.len(), 24);
            s.observe_connection(
                "long",
                "in",
                "alice",
                total,
                false,
                start + Duration::hours(47) + Duration::minutes(45),
            );
            let user = s.pending_user("in", "alice");
            assert_eq!((user.upload, user.download), (480, 960));
            let cutoff = (start + Duration::hours(24)).timestamp();
            for bucket in s.buckets.values() {
                for u in bucket.users.values() {
                    if u.user == "alice" {
                        assert!(bucket.time >= cutoff, "expired identity history retained");
                    }
                }
            }
        }
    }

    #[test]
    fn expired_deferred_user_usage_is_not_moved_to_discovery_time() {
        let sampler = TrafficSampler::new();
        let start = DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        {
            let mut s = sampler.inner.lock().unwrap();
            s.ingest(
                "api",
                &ClashTrafficResponse {
                    upload_total: 0,
                    download_total: 0,
                    connections: vec![],
                },
                start - Duration::minutes(1),
            );
            s.observe_connection("closed", "in", "", [100, 200], false, start);
            s.close_connection("closed");
            s.observe_connection(
                "closed",
                "in",
                "alice",
                [105, 210],
                false,
                start + Duration::hours(25),
            );
            let user = s.pending_user("in", "alice");
            assert_eq!((user.upload, user.download), (5, 10));
            s.observe_connection(
                "closed",
                "in",
                "alice",
                [105, 210],
                false,
                start + Duration::hours(26),
            );
            let user = s.pending_user("in", "alice");
            assert_eq!((user.upload, user.download), (5, 10));
        }
    }

    #[test]
    fn deferred_user_window_has_hard_bound_after_clock_rollback() {
        let start = DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut entry = LedgerEntry::new(start);
        for hour in 0..30 {
            entry.unassigned.insert(
                (start + Duration::hours(hour)).timestamp(),
                [1, 2],
            );
        }
        entry.prune_unassigned(start);
        assert_eq!(entry.unassigned.len(), 24);
        assert_eq!(entry.user_counted, [6, 12]);
        entry.prune_unassigned(start);
        assert_eq!(entry.user_counted, [6, 12]);
    }
}
