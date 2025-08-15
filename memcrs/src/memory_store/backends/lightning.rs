use std::alloc::System;

use lightning::map::{Map, PtrHashMap};

use crate::{
    cache::{
        cache::{CacheMetaData, Record, SetStatus},
        error::CacheError,
    },
    memory_store::store::Peripherals,
};

use super::StorageBackend;
use crate::ffi::unified_str::{UnifiedStr, UnifiedStrHasher, UnifiedStrLarge};

pub struct LightningBackend(PtrHashMap<UnifiedStr, UnifiedStrLarge, System, UnifiedStrHasher>);

impl StorageBackend for LightningBackend {
    fn init(cap: usize) -> Self {
        Self(PtrHashMap::with_capacity_and_hasher(
            cap.next_power_of_two(),
            UnifiedStrHasher::new(),
        ))
    }

    fn get(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        let ukey = UnifiedStr::from_bytes(&key[..]);
        match self.0.get_ref(&ukey) {
            Some(v) => v.to_record().ok_or(CacheError::NotFound),
            None => Err(CacheError::NotFound),
        }
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        let ukey = UnifiedStr::from_bytes(&key[..]);
        self.0.remove_rt_ref(&ukey).and_then(|v| v.to_record())
    }

    fn set(
        &self,
        key: crate::memcache::store::KeyType,
        mut record: crate::memcache::store::Record,
        peripherals: &Peripherals,
    ) -> crate::cache::error::Result<crate::cache::cache::SetStatus> {
        let uval = UnifiedStrLarge::from_record(&record);
        let ukey = UnifiedStr::from_bytes(&key[..]);
        if record.header.cas > 0 {
            record.header.cas += 1;
            record.header.timestamp = peripherals.timestamp();
            let cas = record.header.cas;
            self.0.insert_no_rt(ukey, uval);
            Ok(SetStatus { cas })
        } else {
            self.0.insert_no_rt(ukey, uval);
            Ok(SetStatus { cas: 0 })
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
                .remove_rt_ref(&ukey)
                .and_then(|v| v.to_record())
                .map(|mut record| {
                    record.header = header;
                    record
                })
                .ok_or(CacheError::NotFound);
        } else {
            // No CAS tracking when storing value-only; emulate key-exists behavior
            return Err(CacheError::KeyExists);
        }
    }

    fn flush(&self, header: crate::cache::cache::CacheMetaData) {
        if header.time_to_live == 0 {
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
            .filter(|(k, v)| {
                let rec = v.to_record().unwrap_or_else(|| Record {
                    header: CacheMetaData::new(0, 0, 0),
                    value: Vec::new(),
                });
                f(&k.as_bytes_trimmed().to_vec(), &rec)
            })
            .map(|(k, _v)| k.as_bytes_trimmed().to_vec())
            .collect()
    }
}
