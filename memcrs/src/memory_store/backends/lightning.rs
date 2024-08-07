use std::{alloc::System, hash::RandomState};

use lightning::map::{Map, PtrHashMap};

use crate::{
    cache::{
        cache::{KeyType, Record, SetStatus},
        error::CacheError,
    },
    memory_store::store::Peripherals,
};

use super::StorageBackend;

pub struct LightningBackend(PtrHashMap<KeyType, Record, System, RandomState>);

impl StorageBackend for LightningBackend {
    fn init(cap: usize) -> Self {
        Self(PtrHashMap::with_capacity_and_hasher(
            cap.next_power_of_two(),
            RandomState::new(),
        ))
    }

    fn get(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        match self.0.get_ref(key) {
            Some(rv) => Ok(rv.clone()),
            None => Err(CacheError::NotFound),
        }
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        self.0.remove_rt_ref(&key).map(|rv| rv.clone())
    }

    fn set(
        &self,
        key: crate::memcache::store::KeyType,
        mut record: crate::memcache::store::Record,
        peripherals: &Peripherals,
    ) -> crate::cache::error::Result<crate::cache::cache::SetStatus> {
        if record.header.cas > 0 {
            match self.0.lock(&key) {
                Some(mut key_value) => {
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
                    self.0.insert_no_rt(key, record);
                    Ok(SetStatus { cas })
                }
            }
        } else {
            self.0.insert_no_rt(key, record);
            Ok(SetStatus { cas: 0 })
        }
    }

    fn delete(
        &self,
        key: crate::memcache::store::KeyType,
        header: crate::cache::cache::CacheMetaData,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        if header.cas == 0 {
            return self
                .0
                .remove_rt_ref(&key)
                .map(|rv| rv.clone())
                .ok_or(CacheError::NotFound);
        } else {
            match self.0.lock(&key) {
                Some(record) => {
                    if record.header.cas == header.cas {
                        return Ok(record.remove());
                    } else {
                        return Err(CacheError::KeyExists);
                    }
                }
                None => Err(CacheError::NotFound),
            }
        }
    }

    fn flush(&self, header: crate::cache::cache::CacheMetaData) {
        if header.time_to_live > 0 {
            for k in self.0.keys() {
                if let Some(mut value) = self.0.lock(&k) {
                    value.header.time_to_live = header.time_to_live;
                }
            }
        } else {
            self.0.clear();
        }
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
