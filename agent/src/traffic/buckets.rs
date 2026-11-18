//! UTC-minute traffic buckets and immutable accounting reports.

use std::collections::HashMap;

use chrono::{DateTime, Timelike, Utc};
use uuid::Uuid;

use crate::protocol::{
    PortTrafficSnapshot, TrafficAck, TrafficReport, TrafficSnapshot, TrafficTimeBucket,
    UserTrafficSnapshot, ACCOUNTING_VERSION,
};

use super::ledger::UserTrafficKey;
use super::TrafficSamplerInner;

#[derive(Clone, Debug)]
pub(crate) struct CollectedTrafficBucket {
    pub time: i64,
    pub upload: u64,
    pub download: u64,
    pub upload_rate: u64,
    pub download_rate: u64,
    pub tcp_connections: i32,
    pub udp_connections: i32,
    pub ports: HashMap<String, PortTrafficSnapshot>,
    pub users: HashMap<UserTrafficKey, UserTrafficSnapshot>,
}

impl CollectedTrafficBucket {
    fn new(time: i64) -> Self {
        Self {
            time,
            upload: 0,
            download: 0,
            upload_rate: 0,
            download_rate: 0,
            tcp_connections: 0,
            udp_connections: 0,
            ports: HashMap::new(),
            users: HashMap::new(),
        }
    }

    fn to_time_bucket(&self) -> TrafficTimeBucket {
        TrafficTimeBucket {
            time: self.time,
            upload: self.upload,
            download: self.download,
            upload_rate: self.upload_rate,
            download_rate: self.download_rate,
            tcp_connections: self.tcp_connections,
            udp_connections: self.udp_connections,
            ports: Vec::new(),
            users: Vec::new(),
        }
    }
}

fn truncate_minute(t: DateTime<Utc>) -> DateTime<Utc> {
    t.with_second(0)
        .and_then(|t| t.with_nanosecond(0))
        .unwrap_or(t)
}

impl TrafficSamplerInner {
    /// Callers hold the lock. Collection runs regardless of panel connectivity.
    pub(crate) fn bucket(&mut self, now: DateTime<Utc>) -> &mut CollectedTrafficBucket {
        let key = truncate_minute(now).timestamp();
        self.buckets
            .entry(key)
            .or_insert_with(|| CollectedTrafficBucket::new(key))
    }

    pub(crate) fn add_port_bucket(
        &mut self,
        now: DateTime<Utc>,
        inbound: &str,
        delta: [u64; 2],
    ) {
        let b = self.bucket(now);
        let p = b.ports.entry(inbound.to_string()).or_insert_with(|| {
            PortTrafficSnapshot {
                inbound: inbound.to_string(),
                upload: 0,
                download: 0,
                upload_rate: 0,
                download_rate: 0,
            }
        });
        p.inbound = inbound.to_string();
        p.upload += delta[0];
        p.download += delta[1];
    }

    pub(crate) fn add_user_bucket(
        &mut self,
        now: DateTime<Utc>,
        key: &UserTrafficKey,
        delta: [u64; 2],
    ) {
        let b = self.bucket(now);
        let u = b.users.entry(key.clone()).or_insert_with(|| UserTrafficSnapshot {
            inbound: key.inbound.clone(),
            user: key.user.clone(),
            upload: 0,
            download: 0,
        });
        u.inbound = key.inbound.clone();
        u.user = key.user.clone();
        u.upload += delta[0];
        u.download += delta[1];
    }

    /// Freezes one bounded batch. Only `acknowledge_report` clears it.
    pub(crate) fn build_accounting_snapshot(&mut self) -> TrafficSnapshot {
        let mut snapshot = self.snapshot().unwrap_or(TrafficSnapshot {
            unavailable: true,
            ..Default::default()
        });
        if self.report.is_none() && !self.buckets.is_empty() {
            if self.accounting_session.is_empty() {
                self.accounting_session = Uuid::new_v4().to_string();
            }
            let mut keys: Vec<i64> = self.buckets.keys().copied().collect();
            keys.sort_unstable();
            if keys.len() > 128 {
                keys.truncate(128);
            }
            self.report_sequence += 1;
            let mut report = TrafficReport {
                session: self.accounting_session.clone(),
                sequence: self.report_sequence,
                buckets: Vec::new(),
            };
            let mut entries = 0usize;
            let mut empty_keys = Vec::new();
            for key in keys {
                let Some(b) = self.buckets.get_mut(&key) else {
                    continue;
                };
                let mut out = b.to_time_bucket();
                let port_tags: Vec<String> = b.ports.keys().cloned().collect();
                for tag in port_tags {
                    if entries >= 4096 {
                        break;
                    }
                    if let Some(p) = b.ports.remove(&tag) {
                        out.ports.push(p);
                        entries += 1;
                    }
                }
                let user_keys: Vec<UserTrafficKey> = b.users.keys().cloned().collect();
                for uk in user_keys {
                    if entries >= 4096 {
                        break;
                    }
                    if let Some(u) = b.users.remove(&uk) {
                        out.users.push(u);
                        entries += 1;
                    }
                }
                out.ports.sort_by(|a, b| a.inbound.cmp(&b.inbound));
                out.users.sort_by(|a, b| {
                    a.inbound
                        .cmp(&b.inbound)
                        .then_with(|| a.user.cmp(&b.user))
                });
                report.buckets.push(out);
                // Node total emitted once; remaining port/user rows keep same UTC timestamp.
                b.upload = 0;
                b.download = 0;
                b.upload_rate = 0;
                b.download_rate = 0;
                b.tcp_connections = 0;
                b.udp_connections = 0;
                if b.ports.is_empty() && b.users.is_empty() {
                    empty_keys.push(key);
                }
                if entries >= 4096 {
                    break;
                }
            }
            for key in empty_keys {
                self.buckets.remove(&key);
            }
            self.report = Some(report);
            self.pending_ports.clear();
            self.pending_users.clear();
            self.peak_upload = 0;
            self.peak_download = 0;
        }
        snapshot.accounting_version = ACCOUNTING_VERSION;
        snapshot.report = self.report.clone();
        snapshot.ports.clear();
        snapshot.users.clear();
        snapshot
    }

    pub(crate) fn acknowledge_report(&mut self, ack: TrafficAck) {
        if let Some(report) = &self.report {
            if report.session == ack.session && report.sequence == ack.sequence {
                self.report = None;
            }
        }
    }
}

impl super::TrafficSampler {
    pub(crate) fn accounting_snapshot_inner(&self) -> TrafficSnapshot {
        let mut s = self.inner.lock().expect("traffic lock");
        s.build_accounting_snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traffic::{ClashTrafficResponse, TrafficSampler};
    use chrono::Duration;

    #[test]
    fn offline_traffic_keeps_utc_reset_boundary() {
        let sampler = TrafficSampler::new();
        let reset = DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let report = {
            let mut s = sampler.inner.lock().unwrap();
            s.ingest(
                "api",
                &ClashTrafficResponse {
                    upload_total: 0,
                    download_total: 0,
                    connections: vec![],
                },
                reset - Duration::minutes(2),
            );
            s.ingest(
                "api",
                &ClashTrafficResponse {
                    upload_total: 1000,
                    download_total: 0,
                    connections: vec![],
                },
                reset - Duration::minutes(1),
            );
            s.ingest(
                "api",
                &ClashTrafficResponse {
                    upload_total: 1200,
                    download_total: 0,
                    connections: vec![],
                },
                reset + Duration::minutes(1),
            );
            drop(s);
            let snap = sampler.accounting_snapshot();
            snap.report.unwrap()
        };
        let mut before = 0u64;
        let mut after = 0u64;
        for b in &report.buckets {
            if b.time < reset.timestamp() {
                before += b.upload;
            } else {
                after += b.upload;
            }
        }
        assert_eq!((before, after), (1000, 200));
        {
            let mut s = sampler.inner.lock().unwrap();
            s.available = false;
        }
        let snap = sampler.accounting_snapshot();
        assert!(snap.unavailable);
        assert_eq!(snap.report.as_ref().unwrap().sequence, report.sequence);
    }

    #[test]
    fn late_user_identity_preserves_original_bucket() {
        let sampler = TrafficSampler::new();
        let reset = DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z")
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
                reset - Duration::minutes(2),
            );
            s.observe_connection(
                "c",
                "in",
                "",
                [100, 200],
                false,
                reset - Duration::minutes(1),
            );
            s.observe_connection(
                "c",
                "in",
                "alice",
                [120, 240],
                false,
                reset + Duration::minutes(1),
            );
        }
        let report = sampler.accounting_snapshot().report.unwrap();
        let mut before = 0u64;
        let mut after = 0u64;
        for b in &report.buckets {
            for u in &b.users {
                if b.time < reset.timestamp() {
                    before += u.upload;
                } else {
                    after += u.upload;
                }
            }
        }
        assert_eq!((before, after), (100, 20));
    }

    #[test]
    fn traffic_reports_wait_for_panel_acknowledgement() {
        let sampler = TrafficSampler::new();
        let now = Utc::now();
        {
            let mut s = sampler.inner.lock().unwrap();
            s.available = true;
            s.have_sample = true;
            s.last_sample = now;
            s.bucket(now).upload = 100;
            s.add_port_bucket(now, "in", [100, 0]);
            s.add_user_bucket(
                now,
                &UserTrafficKey {
                    inbound: "in".into(),
                    user: "alice".into(),
                },
                [100, 0],
            );
        }
        let first = sampler.accounting_snapshot();
        let report = first.report.clone().unwrap();
        assert_eq!(report.buckets[0].upload, 100);
        // Second call must return the same frozen report.
        let second = sampler.accounting_snapshot();
        assert_eq!(second.report.as_ref().unwrap().sequence, report.sequence);
        {
            let mut s = sampler.inner.lock().unwrap();
            s.bucket(now).upload += 50;
        }
        // Unmatched ack must not clear.
        sampler.acknowledge_report(TrafficAck {
            session: report.session.clone(),
            sequence: report.sequence + 1,
        });
        assert!(sampler.accounting_snapshot().report.is_some());
        sampler.acknowledge_report(TrafficAck {
            session: report.session.clone(),
            sequence: report.sequence,
        });
        let next = sampler.accounting_snapshot();
        let next_report = next.report.unwrap();
        assert_eq!(next_report.sequence, report.sequence + 1);
        assert_eq!(next_report.buckets[0].upload, 50);
    }
}
