use std::fs::File;
use std::sync::atomic::Ordering::*;
use std::{
    collections::HashMap,
    mem::{replace, swap},
    sync::{atomic::AtomicBool, Arc},
};

use parking_lot::Mutex;

use crate::protocol::binary_codec::BinaryRequest;

pub struct MasterRecorder {
    enabled: AtomicBool,
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
            self.master
                .all_recordings
                .lock()
                .insert(self.connection_id, records);
        }
    }
}

impl MasterRecorder {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            all_recordings: Mutex::new(HashMap::new()),
        }
    }

    pub fn start(&self) {
        self.enabled.store(true, Release);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Relaxed)
    }

    pub fn dump(&self, name: &str) -> bincode::Result<u32>  {
        let mut all_recordings = self.all_recordings.lock();
        let conns = all_recordings.len();
        for (conn_id, reqs) in all_recordings.iter() {
            let filename = format!("{}-{}.bin", name, conn_id);
            let mut f = File::create(filename).unwrap();
            bincode::serialize_into(&mut f, reqs)?;
        }
        all_recordings.clear();
        self.enabled.store(false, Relaxed);
        Ok(conns as u32 )
    }
}
