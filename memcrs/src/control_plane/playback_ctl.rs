use std::time::Instant;

use parking_lot::Mutex;

#[derive(Clone)]
pub struct PlaybackStatus {
    name: String,
    start_time: Instant,
    finish_time: Option<Instant>,
}

pub struct Playback {
    status: Mutex<PlaybackStatus>,
}

impl Playback {
    pub fn new() -> Self {
        Self {
            status: Mutex::new(PlaybackStatus {
                name: "".to_string(),
                start_time: Instant::now(),
                finish_time: Some(Instant::now()),
            }),
        }
    }

    pub fn start(&self, name: &String) -> bool {
        let mut stats = self.status.lock();
        if stats.finish_time.is_some() {
            stats.name = name.clone();
            stats.start_time = Instant::now();
            stats.finish_time = None;
            return true;
        } else {
            return false;
        }
    }

    pub fn stop(&self) {
        let mut stats = self.status.lock();
        stats.finish_time = Some(Instant::now());
    }

    pub fn status(&self) -> PlaybackStatus {
        self.status.lock().clone()
    }
}
