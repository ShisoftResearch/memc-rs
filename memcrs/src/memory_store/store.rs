use crate::cache::cache::{
    impl_details, Cache, CacheMetaData, CachePredicate, KeyType, Record, RemoveIfResult, SetStatus,
};
use crate::cache::error::Result;
use crate::server::timer;
use serde_derive::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::backends::StorageBackend;
use super::backends::*;
pub type DefaultMemoryStore = MemoryStore<lightning::LightningBackend>;

#[derive(Serialize, Deserialize)]
pub struct BytesCodec(Vec<u8>);

#[derive(Serialize, Deserialize)]
pub struct RecordCodec {
    header: CacheMetaData,
    data: BytesCodec,
}

pub struct MemoryStore<M: StorageBackend> {
    memory: M,
    peripherals: Peripherals,
}

pub struct Peripherals {
    timer: Arc<dyn timer::Timer + Send + Sync>,
    cas_id: AtomicU64,
}

impl<M: StorageBackend> MemoryStore<M> {
    pub fn new(timer: Arc<dyn timer::Timer + Send + Sync>, cap: usize) -> Self {
        MemoryStore {
            memory: M::init(cap),
            peripherals: Peripherals {
                timer,
                cas_id: AtomicU64::new(1),
            },
        }
    }
}

impl<M: StorageBackend> impl_details::CacheImplDetails for MemoryStore<M> {
    fn get_by_key(&self, key: &KeyType) -> Result<Record> {
        self.memory.get(key)
    }

    fn check_if_expired(&self, key: &KeyType, record: &Record) -> bool {
        let current_time = self.peripherals.timestamp();

        if record.header.time_to_live == 0 {
            return false;
        }

        if record.header.timestamp + record.header.time_to_live > current_time {
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
        self.memory.remove(key)
    }

    fn set(&self, key: KeyType, record: Record) -> Result<SetStatus> {
        self.memory.set(key, record, &self.peripherals)
    }

    fn delete(&self, key: KeyType, header: CacheMetaData) -> Result<Record> {
        self.memory.delete(key, header)
    }

    fn flush(&self, header: CacheMetaData) {
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

impl Peripherals {
    #[inline(always)]
    pub fn get_cas_id(&self) -> u32 {
        self.cas_id.fetch_add(1, Ordering::Relaxed) as u32
    }

    #[inline(always)]
    pub fn timestamp(&self) -> u32 {
        self.timer.timestamp() as u32
    }
}
