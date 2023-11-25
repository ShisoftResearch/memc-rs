use std::time::Instant;

use parking_lot::Mutex;

#[derive(Clone)]
pub struct PlaybackStatus {
    name: String,
    start_time: Option<Instant>,
    finish_time: Option<Instant>,
    report: Option<PlaybackReport>,
}

#[derive(Clone)]
pub struct PlaybackReport {
    pub ops: u64,
    pub throughput: f64,
    pub c90: u64,
    pub c99: u64,
    pub c99_9: u64,
    pub c99_99: u64
}

pub struct Playback {
    status: Mutex<PlaybackStatus>,
}

impl Playback {
    pub fn new() -> Self {
        Self {
            status: Mutex::new(PlaybackStatus {
                name: "".to_string(),
                start_time: None,
                finish_time: None,
                report: None
            }),
        }
    }

    pub fn start(&self, name: &String) -> bool {
        let mut stats = self.status.lock();
        if stats.start_time.is_none() {
            stats.name = name.clone();
            stats.start_time = Some(Instant::now());
            stats.finish_time = None;
            return true;
        } else {
            return false;
        }
    }

    pub fn stop(&self, report: PlaybackReport) {
        let mut stats = self.status.lock();
        if stats.start_time.is_some() {
            stats.finish_time = Some(Instant::now());
            stats.start_time = None;
            stats.report = Some(report)
        }
    }

    pub fn status(&self) -> PlaybackStatus {
        self.status.lock().clone()
    }
}
