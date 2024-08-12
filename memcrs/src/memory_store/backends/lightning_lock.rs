use std::{alloc::System, hash::RandomState};

use lightning::map::{LockingHashMap, Map};

use crate::{
    cache::{
        cache::{KeyType, Record, SetStatus},
        error::CacheError,
    },
    memory_store::store::Peripherals,
};

use super::StorageBackend;

pub struct LightningLockBackend(LockingHashMap<KeyType, Record, System, RandomState>);

impl StorageBackend for LightningLockBackend {
    fn init(cap: usize) -> Self {
        Self(LockingHashMap::with_capacity_and_hasher(
            cap.next_power_of_two(),
            RandomState::new(),
        ))
    }

    fn get(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        match self.0.get(key) {
            Some(rv) => Ok(rv),
            None => Err(CacheError::NotFound),
        }
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        self.0.remove(&key)
    }

    fn set(
        &self,
        key: crate::memcache::store::KeyType,
        mut record: crate::memcache::store::Record,
        peripherals: &Peripherals,
    ) -> crate::cache::error::Result<crate::cache::cache::SetStatus> {
        if record.header.cas > 0 {
            // match self.0.lock(&key) {
            //     Some(mut key_value) => {
            //         if key_value.header.cas != record.header.cas {
            //             Err(CacheError::KeyExists)
            //         } else {
            //             record.header.cas += 1;
            //             record.header.timestamp = peripherals.timestamp();
            //             let cas = record.header.cas;
            //             *key_value = record;
            //             Ok(SetStatus { cas })
            //         }
            //     }
            //     None => {
            //         record.header.cas += 1;
            //         record.header.timestamp = peripherals.timestamp();
            //         let cas = record.header.cas;
            //         self.0.insert(key, record);
            //         Ok(SetStatus { cas })
            //     }
            // }
            unimplemented!()
        } else {
            self.0.insert(key, record);
            Ok(SetStatus { cas: 0 })
        }
    }

    fn delete(
        &self,
        key: crate::memcache::store::KeyType,
        header: crate::cache::cache::CacheMetaData,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        if header.cas == 0 {
            return self.0.remove(&key).ok_or(CacheError::NotFound);
        } else {
            // match self.0.lock(&key) {
            //     Some(record) => {
            //         if record.header.cas == header.cas {
            //             return Ok(record.remove());
            //         } else {
            //             return Err(CacheError::KeyExists);
            //         }
            //     }
            //     None => Err(CacheError::NotFound),
            // }
            unimplemented!()
        }
    }

    fn flush(&self, header: crate::cache::cache::CacheMetaData) {
        self.0.clear();
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn predict_keys(
        &self,
        f: &mut crate::cache::cache::CachePredicate,
    ) -> Vec<crate::memcache::store::KeyType> {
        self.0
            .entries()
            .into_iter()
            .filter(|(k, v)| f(k, v))
            .map(|(k, _v)| k)
            .collect()
    }
}
