use crate::cache::cache::{
    impl_details, Cache, CacheMetaData, CachePredicate, KeyType, Record,
    RemoveIfResult, SetStatus, ValueType,
};
use crate::cache::error::{CacheError, Result};
use crate::server::timer;
use std::fs::File;
use std::io::BufWriter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bincode::serialize_into;
use lightning::map::{PtrHashMap, Map};
use parking_lot::Mutex;
use serde_derive::{Serialize, Deserialize};

type Storage = PtrHashMap<KeyType, Record>;
type Recorder = PtrHashMap<KeyType, Arc<Mutex<Vec<(char, Option<Record>)>>>>;

#[derive(Serialize, Deserialize)]
pub struct BytesCodec(Vec<u8>);

#[derive(Serialize, Deserialize)]
pub struct RecordCodec{
    header: CacheMetaData,
    data: BytesCodec
}

pub struct MemoryStore {
    memory: Storage,
    recorder: Recorder,
    timer: Arc<dyn timer::Timer + Send + Sync>,
    cas_id: AtomicU64,
}

impl MemoryStore {
    pub fn new(timer: Arc<dyn timer::Timer + Send + Sync>, cap: usize) -> MemoryStore {
        MemoryStore {
            memory: PtrHashMap::with_capacity(cap.next_power_of_two()),
            recorder: PtrHashMap::with_capacity(cap.next_power_of_two()),
            timer,
            cas_id: AtomicU64::new(1),
        }
    }

    fn get_cas_id(&self) -> u64 {
        self.cas_id.fetch_add(1, Ordering::Release)
    }

    fn push_record(&self, key: &KeyType, op: char, rec: Option<&Record>) {
        loop {
            if let Some(recs_mutex) = self.recorder.get(key) {
                let mut recs = recs_mutex.lock();
                recs.push((op, rec.cloned()));
                break;
            } else {
                self.recorder.try_insert(key.clone(), Arc::new(Mutex::new(vec![])));
            }
        }
    }

    fn store_get(&self, key: &KeyType) -> Result<Record> {
        self.memory.get(key).ok_or(CacheError::NotFound)
    }
    fn store_remove(&self, key: &KeyType)  -> Option<Record> {
        self.memory.remove(&key)
    }
    fn store_set(&self, key: KeyType, mut record: Record) -> Result<SetStatus> {
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
    fn store_delete(&self, key: KeyType, header: CacheMetaData) -> Result<Record> {
        if header.cas == 0 {
            return self.memory.remove(&key).ok_or(CacheError::NotFound)
        } else {  
            match self.memory.lock(&key) {
                Some(record) => {
                    if record.header.cas == header.cas {
                        return Ok(record.remove());
                    } else {
                        return Err(CacheError::KeyExists);
                    }
                }
                None => Err(CacheError::NotFound)
            }
        }
    }
    fn dump_recording(&self, filename: &String) {
        let mut all = self.recorder
            .entries()
            .into_iter()
            .map(|(k, v)| {
                let vg = v.lock();
                let val = vg.clone();
                (k, val)
            })
            .collect::<Vec<_>>();
        let mut f = BufWriter::new(File::create(filename).unwrap());
        serialize_into(&mut f, &all).unwrap();
    }
}

impl impl_details::CacheImplDetails for MemoryStore {
    fn get_by_key(&self, key: &KeyType) -> Result<Record> {
        self.push_record(key, 'g', None);
        self.store_get(key)
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
        self.push_record(&key, 'r', None);
        self.store_remove(key)
    }

    fn set(&self, key: KeyType, record: Record) -> Result<SetStatus> {
        self.push_record(&key, 's', Some(&record));
        self.store_set(key, record)
    }

    fn delete(&self, key: KeyType, header: CacheMetaData) -> Result<Record> {
        self.push_record(&key, 'd', Some(&Record {
            header: header.clone(), 
            value: ValueType::new()
        }));
        self.store_delete(key, header)
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
