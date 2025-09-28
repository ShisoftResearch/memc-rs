use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

use crate::protocol::binary_codec::BinaryRequest;

#[derive(Clone, Serialize, Deserialize)]
pub struct PlaybackStatus {
    name: String,
    start_time: u64,
    finish_time: Option<u64>,
    report: Option<PlaybackReport>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PlaybackReport {
    pub ops: u64,
    pub throughput: f64,
    pub avg: f64,
    pub c50: u64,
    pub c90: u64,
    pub c99: u64,
    pub c99_9: u64,
    pub c99_99: u64,
    pub min: u64,
    pub max: u64,
}

pub struct Playback {
    status: Mutex<PlaybackStatus>,
    pub req_history: Mutex<Vec<Vec<(u64, Vec<BinaryRequest>)>>>,
}

impl Playback {
    pub fn new() -> Self {
        let current = current_time_mills();
        Self {
            status: Mutex::new(PlaybackStatus {
                name: "".to_string(),
                start_time: current,
                finish_time: Some(current),
                report: None,
            }),
            req_history: Mutex::new(vec![]),
        }
    }

    pub fn start(&self, name: &String) -> bool {
        let mut stats = self.status.lock();
        if stats.finish_time.is_some() {
            stats.name = name.clone();
            stats.start_time = current_time_mills();
            stats.finish_time = None;
            stats.report = None;
            return true;
        } else {
            return false;
        }
    }

    pub fn stop(&self, report: PlaybackReport) {
        let mut stats = self.status.lock();
        if stats.finish_time.is_none() {
            stats.finish_time = Some(current_time_mills());
            stats.report = Some(report)
        }
    }

    pub fn status(&self) -> PlaybackStatus {
        self.status.lock().clone()
    }
}

fn current_time_mills() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_millis() as u64
}
