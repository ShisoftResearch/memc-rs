use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use super::StorageBackend;
use crate::{
    cache::{
        cache::{KeyType, Record, SetStatus},
        error::CacheError,
    },
    memory_store::store::Peripherals,
};

pub struct RwMapBackend(RwLock<HashMap<KeyType, Record>>);

impl StorageBackend for RwMapBackend {
    fn init(cap: usize) -> Self {
        Self(RwLock::new(HashMap::with_capacity(cap.next_power_of_two())))
    }

    fn get(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        self.0
            .read()
            .get(key)
            .map(|v| v.clone())
            .ok_or(CacheError::NotFound)
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        self.0.write().remove(key)
    }

    fn set(
        &self,
        key: crate::memcache::store::KeyType,
        mut record: crate::memcache::store::Record,
        peripherals: &Peripherals,
    ) -> crate::cache::error::Result<crate::cache::cache::SetStatus> {
        //trace!("Set: {:?}", &record.header);
        let mut lock = self.0.write();
        if record.header.cas > 0 {
            match lock.get_mut(&key) {
                Some(key_value) => {
                    if key_value.header.cas != record.header.cas {
                        Err(CacheError::KeyExists)
                    } else {
                        record.header.cas += 1;
                        record.header.timestamp = peripherals.timestamp();
                        let cas = record.header.cas;
                        *key_value = record;
                        Ok(SetStatus { cas })
                    }
                }
                None => {
                    record.header.cas += 1;
                    record.header.timestamp = peripherals.timestamp();
                    let cas = record.header.cas;
                    lock.insert(key, record);
                    Ok(SetStatus { cas })
                }
            }
        } else {
            lock.insert(key, record);
            Ok(SetStatus { cas: 0 })
        }
    }

    fn delete(
        &self,
        key: crate::memcache::store::KeyType,
        header: crate::cache::cache::CacheMetaData,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        let mut lock = self.0.write();
        if let Some(record) = lock.get(&key) {
            let result = header.cas == 0 || record.header.cas == header.cas;
            if result {
                let val = lock.remove(&key).unwrap();
                return Ok(val);
            } else {
                return Err(CacheError::KeyExists);
            }
        } else {
            return Err(CacheError::NotFound);
        }
    }

    fn flush(&self, header: crate::cache::cache::CacheMetaData) {
        if header.time_to_live > 0 {
            self.0.write().iter_mut().for_each(|(_key, mut value)| {
                value.header.time_to_live = header.time_to_live;
            });
        } else {
            self.0.write().clear();
        }
    }

    fn len(&self) -> usize {
        self.0.read().len()
    }

    fn predict_keys(
        &self,
        f: &mut crate::cache::cache::CachePredicate,
    ) -> Vec<crate::memcache::store::KeyType> {
        self.0
            .read()
            .iter()
            .filter(|record| f(record.0, record.1))
            .map(|record| record.0.clone())
            .collect()
    }
}
