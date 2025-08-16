use std::{alloc::System, mem};

use lightning::map::{LockingHashMap, Map};

use crate::{
    cache::{
        cache::{CacheMetaData, Record},
        error::CacheError,
    },
    memory_store::store::Peripherals,
};

use super::StorageBackend;
use crate::ffi::unified_str::{UnifiedStr, UnifiedStrHasher, MapValue};
use bytes::Bytes;

pub struct LightningLockBackend(
    LockingHashMap<UnifiedStr, MapValue, System, UnifiedStrHasher>,
);

impl StorageBackend for LightningLockBackend {
    fn init(cap: usize) -> Self {
        Self(LockingHashMap::with_capacity_and_hasher(
            cap.next_power_of_two(),
            UnifiedStrHasher::new(),
        ))
    }

    fn get(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        let ukey = UnifiedStr::from_bytes(&key[..]);
        match self.0.get(&ukey) {
            Some(v) => {
                let shadow = v.to_record();
                let val = shadow.clone();
                mem::forget(shadow);
                Ok(val)
            },
            None => Err(CacheError::NotFound),
        }
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        let ukey = UnifiedStr::from_bytes(&key[..]);
        self.0.remove(&ukey).map(|v| v.to_record())
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
            self.0.insert(ukey, uval);
            Ok(crate::cache::cache::SetStatus { cas })
        } else {
            let uval = MapValue::from_record(record);
            self.0.insert(ukey, uval);
            Ok(crate::cache::cache::SetStatus { cas: 0 })
        }
    }

    fn delete(
        &self,
        key: crate::memcache::store::KeyType,
        header: crate::cache::cache::CacheMetaData,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        let ukey = UnifiedStr::from_bytes(&key[..]);
        if header.cas == 0 {
            return self
                .0
                .remove(&ukey)
                .map(|v| v.to_record())
                .map(|mut record| {
                    record.header = header;
                    record
                })
                .ok_or(CacheError::NotFound);
        } else {
            return Err(CacheError::KeyExists);
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
            .filter(|(k, v)| {
                let rec = v.to_record();
                f(&Bytes::copy_from_slice(k.as_bytes_trimmed()), &rec)
            })
            .map(|(k, _v)| Bytes::copy_from_slice(k.as_bytes_trimmed()))
            .collect()
    }
}
