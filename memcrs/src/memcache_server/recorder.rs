use std::fs::File;
use std::sync::atomic::Ordering::*;
use std::{
    collections::HashMap,
    mem::{replace, swap},
    sync::{atomic::*, Arc},
};

use parking_lot::Mutex;

use crate::protocol::binary_codec::BinaryRequest;

pub struct MasterRecorder {
    enabled: AtomicBool,
    connection_counter: AtomicU64,
    all_recordings: Mutex<HashMap<u64, Vec<BinaryRequest>>>,
}

pub struct ConnectionRecorder {
    operations: Mutex<Vec<BinaryRequest>>,
    master: Arc<MasterRecorder>,
    connection_id: u64,
    enabled: bool,
}

impl ConnectionRecorder {
    pub fn new(connection_id: u64, enabled: bool, master: &Arc<MasterRecorder>) -> Self {
        if enabled {
            info!("Enabled recorder created #{}", connection_id);
        } else {
            debug!("Recording not enabled for #{}", connection_id);
        }
        ConnectionRecorder {
            operations: Mutex::new(vec![]),
            master: master.clone(),
            connection_id,
            enabled,
        }
    }

    pub fn push_record(&self, req: &BinaryRequest) {
        if self.enabled {
            self.operations.lock().push(req.clone())
        }
    }

    pub fn stop(&self) {
        if self.enabled {
            let mut self_records = self.operations.lock();
            let records = replace(&mut *self_records, vec![]);
            let mut all_recordings = self.master.all_recordings.lock();
            let connection_id = self.connection_id;
            assert!(!all_recordings.contains_key(&connection_id));
            all_recordings.insert(self.connection_id, records);
            info!(
                "Moving recording for connection {} after closing",
                self.connection_id
            );
        } else {
            debug!(
                "Nothing get recorded for connection #{}",
                self.connection_id
            );
        }
    }
}

impl MasterRecorder {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            connection_counter: AtomicU64::new(0),
            all_recordings: Mutex::new(HashMap::new()),
        }
    }

    pub fn start(&self) {
        self.enabled.store(true, Release);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Relaxed)
    }

    pub fn incr_conn_id(&self) -> u64 {
        self.connection_counter.fetch_add(1, Relaxed)
    }

    pub fn max_conn_id(&self) -> u64 {
        self.connection_counter.load(Acquire)
    }

    pub fn dump(&self, name: &str) -> bincode::Result<u32> {
        info!("Start dumping recording for '{}'", name);
        let mut all_recordings = self.all_recordings.lock();
        let conns = all_recordings.len();
        for (conn_id, reqs) in all_recordings.iter() {
            let filename = format!("{}-{}.bin", name, conn_id);
            let mut f = File::create(filename).unwrap();
            bincode::serialize_into(&mut f, reqs)?;
            info!(
                "Dump recording '{}' for connection {} completed",
                name, conn_id
            );
        }
        all_recordings.clear();
        self.enabled.store(false, Relaxed);
        Ok(conns as u32)
    }
}
