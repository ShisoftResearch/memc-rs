use std::{collections::HashMap, mem};

use parking_lot::RwLock;

use super::StorageBackend;
use crate::{
    cache::{cache::SetStatus, error::CacheError},
    ffi::unified_str::*,
    memory_store::store::Peripherals,
};
use bytes::Bytes;

pub struct RwMapBackend(RwLock<HashMap<UnifiedStr, MapValue, UnifiedStrHasher>>);

impl StorageBackend for RwMapBackend {
    fn init(cap: usize) -> Self {
        Self(RwLock::new(HashMap::with_capacity_and_hasher(
            cap.next_power_of_two(),
            UnifiedStrHasher::new(),
        )))
    }

    fn get(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        let ukey = UnifiedStr::from_bytes(&key[..]);
        match self.0.read().get(&ukey) {
            Some(v) => Ok(v.to_record_ref().clone()),
            None => Err(CacheError::NotFound),
        }
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        let ukey = UnifiedStr::from_bytes(&key[..]);
        self.0.write().remove(&ukey).map(|v| v.to_record())
    }

    fn set(
        &self,
        key: crate::memcache::store::KeyType,
        mut record: crate::memcache::store::Record,
        peripherals: &Peripherals,
    ) -> crate::cache::error::Result<crate::cache::cache::SetStatus> {
        let ukey = UnifiedStr::from_bytes(&key[..]);
        if record.header.cas > 0 {
            record.header.cas += 1;
            record.header.timestamp = peripherals.timestamp();
            let cas = record.header.cas;
            let uval = MapValue::from_record(record);
            self.0.write().insert(ukey, uval);
            Ok(SetStatus { cas })
        } else {
            let uval = MapValue::from_record(record);
            self.0.write().insert(ukey, uval);
            Ok(SetStatus { cas: 0 })
        }
    }

    fn delete(
        &self,
        key: crate::memcache::store::KeyType,
        header: crate::cache::cache::CacheMetaData,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        let ukey = UnifiedStr::from_bytes(&key[..]);
        let mut lock = self.0.write();
        if let Some(map_value) = lock.get(&ukey) {
            let record = map_value.to_record();
            let result = header.cas == 0 || record.header.cas == header.cas;
            if result {
                let val = lock.remove(&ukey).unwrap().to_record();
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
            self.0.write().iter_mut().for_each(|(_key, map_value)| {
                let mut record = map_value.to_record();
                record.header.time_to_live = header.time_to_live;
                *map_value = MapValue::from_record(record);
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
            .filter(|(unified_key, map_value)| {
                let key_bytes = Bytes::copy_from_slice(unified_key.as_bytes_trimmed());
                let record = map_value.to_record_ref();
                f(&key_bytes, record)
            })
            .map(|(unified_key, _map_value)| Bytes::copy_from_slice(unified_key.as_bytes_trimmed()))
            .collect()
    }
}
