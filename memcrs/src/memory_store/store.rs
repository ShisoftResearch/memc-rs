use crate::cache::cache::{
    impl_details, Cache, CacheMetaData, CachePredicate, KeyType, Record, RemoveIfResult, SetStatus,
    ValueType,
};
use crate::cache::error::{CacheError, Result};
use crate::server::timer;
use std::fs::File;
use std::io::BufWriter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bincode::serialize_into;
use bytes::Bytes;
use lightning::map::{Map, PtrHashMap};
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

use super::backends::StorageBackend;
use super::backends::lightning::LightningBackend;
type Recorder = PtrHashMap<KeyType, Arc<Mutex<Vec<(char, Option<Record>)>>>>;
pub type DefaultMemoryStore = MemoryStore<LightningBackend>;

#[derive(Serialize, Deserialize)]
pub struct BytesCodec(Vec<u8>);

#[derive(Serialize, Deserialize)]
pub struct RecordCodec {
    header: CacheMetaData,
    data: BytesCodec,
}

pub struct MemoryStore<M: StorageBackend> {
    memory: M,
    recorder: Recorder,
    peripherals: Peripherals
}

pub struct Peripherals {
    timer: Arc<dyn timer::Timer + Send + Sync>,
    cas_id: AtomicU64,
}

impl<M: StorageBackend> MemoryStore<M> {
    pub fn new(timer: Arc<dyn timer::Timer + Send + Sync>, cap: usize) -> Self {
        MemoryStore {
            memory: M::init(cap),
            recorder: PtrHashMap::with_capacity(cap.next_power_of_two()),
            peripherals: Peripherals {
                timer,
                cas_id: AtomicU64::new(1),
            }
        }
    }

    fn get_cas_id(&self) -> u64 {
       self.peripherals.get_cas_id()
    }

    fn push_record(&self, key: &KeyType, op: char, rec: Option<&Record>) {
        loop {
            if let Some(recs_mutex) = self.recorder.get(key) {
                let mut recs = recs_mutex.lock();
                recs.push((op, rec.cloned()));
                break;
            } else {
                self.recorder
                    .try_insert(key.clone(), Arc::new(Mutex::new(vec![])));
            }
        }
    }

    fn dump_recording(&self, filename: &String) {
        let all = self
            .recorder
            .entries()
            .into_iter()
            .map(|(k, v)| {
                let vg = v.lock();
                let val = vg.clone();
                let kb = BytesCodec::from_bytes(k);
                let vb = val
                    .into_iter()
                    .map(|(c, v)| (c, v.map(|r| RecordCodec::from_record(r))))
                    .collect::<Vec<_>>();
                (kb, vb)
            })
            .collect::<Vec<_>>();
        let mut f = BufWriter::new(File::create(filename).unwrap());
        serialize_into(&mut f, &all).unwrap();
    }
}

impl<M: StorageBackend> impl_details::CacheImplDetails for MemoryStore<M> {
    fn get_by_key(&self, key: &KeyType) -> Result<Record> {
        self.push_record(key, 'g', None);
        self.memory.get(key)
    }

    fn check_if_expired(&self, key: &KeyType, record: &Record) -> bool {
        let current_time = self.peripherals.timestamp();

        if record.header.time_to_live == 0 {
            return false;
        }

        if record.header.timestamp + (record.header.time_to_live as u64) > current_time {
            return false;
        }
        match self.remove(key) {
            Some(_) => true,
            None => true,
        }
    }
}

impl<M: StorageBackend> Cache for MemoryStore<M> {
    // Removes key value and returns as an option
    fn remove(&self, key: &KeyType) -> Option<Record> {
        self.push_record(&key, 'r', None);
        self.memory.remove(key)
    }

    fn set(&self, key: KeyType, record: Record) -> Result<SetStatus> {
        self.push_record(&key, 's', Some(&record));
        self.memory.set(key, record, &self.peripherals)
    }

    fn delete(&self, key: KeyType, header: CacheMetaData) -> Result<Record> {
        self.push_record(
            &key,
            'd',
            Some(&Record {
                header: header.clone(),
                value: ValueType::new(),
            }),
        );
        self.memory.delete(key, header)
    }

    fn flush(&self, header: CacheMetaData) {
        self.push_record(
            &Bytes::new(),
            'c',
            Some(&Record {
                header: header.clone(),
                value: ValueType::new(),
            }),
        );
        self.memory.flush(header)
    }

    fn remove_if(&self, f: &mut CachePredicate) -> RemoveIfResult {
        let items = self.memory.predict_keys(f);
        let result: Vec<Option<Record>> = items.into_iter().map(|key| self.remove(&key)).collect();
        result
    }

    fn len(&self) -> usize {
        self.memory.len()
    }

    fn is_empty(&self) -> bool {
        self.memory.len() == 0
    }
}

impl BytesCodec {
    fn from_bytes(bytes: Bytes) -> Self {
        Self(bytes.into())
    }
}

impl RecordCodec {
    fn from_record(record: Record) -> Self {
        Self {
            header: record.header,
            data: BytesCodec::from_bytes(record.value),
        }
    }
}

impl Peripherals {
    pub fn get_cas_id(&self) -> u64 {
        self.cas_id.fetch_add(1, Ordering::Release)
    }
    pub fn timestamp(&self) -> u64 {
        self.timer.timestamp()
    }
} 
