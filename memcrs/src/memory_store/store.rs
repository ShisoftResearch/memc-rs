use crate::cache::cache::{
    impl_details, Cache, CacheMetaData, CachePredicate, KeyType, Record,
    RemoveIfResult, SetStatus,
};
use crate::cache::error::{CacheError, Result};
use crate::server::timer;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use lightning::map::{PtrHashMap, Map};

type Storage = PtrHashMap<KeyType, Record>;
pub struct MemoryStore {
    memory: Storage,
    timer: Arc<dyn timer::Timer + Send + Sync>,
    cas_id: AtomicU64,
}

impl MemoryStore {
    pub fn new(timer: Arc<dyn timer::Timer + Send + Sync>) -> MemoryStore {
        MemoryStore {
            memory: PtrHashMap::with_capacity(1024),
            timer,
            cas_id: AtomicU64::new(1),
        }
    }

    fn get_cas_id(&self) -> u64 {
        self.cas_id.fetch_add(1, Ordering::Release)
    }
}

impl impl_details::CacheImplDetails for MemoryStore {
    fn get_by_key(&self, key: &KeyType) -> Result<Record> {
        match self.memory.get(key) {
            Some(record) => Ok(record),
            None => Err(CacheError::NotFound),
        }
    }

    fn check_if_expired(&self, key: &KeyType, record: &Record) -> bool {
        let current_time = self.timer.timestamp();

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

impl Cache for MemoryStore {
    // Removes key value and returns as an option
    fn remove(&self, key: &KeyType) -> Option<Record> {
        self.memory.lock(key).map(|g| g.remove())
    }

    fn set(&self, key: KeyType, mut record: Record) -> Result<SetStatus> {
        //trace!("Set: {:?}", &record.header);
        if record.header.cas > 0 {
            match self.memory.lock(&key) {
                Some(mut key_value) => {
                    if key_value.header.cas != record.header.cas {
                        Err(CacheError::KeyExists)
                    } else {
                        record.header.cas += 1;
                        record.header.timestamp = self.timer.timestamp();
                        let cas = record.header.cas;
                        *key_value = record;
                        Ok(SetStatus { cas })
                    }
                }
                None => {
                    record.header.cas += 1;
                    record.header.timestamp = self.timer.timestamp();
                    let cas = record.header.cas;
                    self.memory.insert(key, record);
                    Ok(SetStatus { cas })
                }
            }
        } else {
            let cas = self.get_cas_id();
            record.header.cas = cas;
            record.header.timestamp = self.timer.timestamp();
            self.memory.insert(key, record);
            Ok(SetStatus { cas })
        }
    }

    fn delete(&self, key: KeyType, header: CacheMetaData) -> Result<Record> {
        match self.memory.lock(&key) {
            Some(record) => {
                if header.cas == 0 || record.header.cas == header.cas {
                    return Ok(record.remove());
                } else {
                    return Err(CacheError::KeyExists);
                }
            }
            None => Err(CacheError::NotFound)
        }
    }

    fn flush(&self, header: CacheMetaData) {
        if header.time_to_live > 0 {
            for k in self.memory.keys() {
                if let Some(mut value) = self.memory.lock(&k) {
                    value.header.time_to_live = header.time_to_live;
                }
            }
        } else {
            self.memory.clear();
        }
    }

    fn remove_if(&self, f: &mut CachePredicate) -> RemoveIfResult {
        let items: Vec<KeyType> = self
            .memory
            .entries()
            .into_iter()
            .filter(|(k, v)| f(k, v))
            .map(|(k, _v)| k)
            .collect();

        let result: Vec<Option<Record>> =
            items.into_iter().map(|key| self.remove(&key)).collect();
        result
    }

    fn len(&self) -> usize {
        self.memory.len()
    }

    fn is_empty(&self) -> bool {
        self.memory.len() == 0
    }
}
