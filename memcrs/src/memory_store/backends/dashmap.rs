use super::StorageBackend;
use crate::{
    cache::{
        cache::SetStatus,
        error::CacheError,
    }, ffi::unified_str::*, memory_store::store::Peripherals
};
use dashmap::mapref::multiple::RefMulti;
use dashmap::DashMap;
use bytes::Bytes;
use std::mem;

pub struct DashMapBackend(DashMap<UnifiedStr, MapValue, UnifiedStrHasher>);

impl StorageBackend for DashMapBackend {
    fn init(cap: usize) -> Self {
        Self(DashMap::with_capacity_and_hasher(
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
                Ok(v.to_record_ref().clone())
            },
            None => Err(CacheError::NotFound),
        }
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        let ukey = UnifiedStr::from_bytes(&key[..]);
        self.0.remove(&ukey).map(|(_, v)| v.to_record())
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
            Ok(SetStatus { cas })
        } else {
            let uval = MapValue::from_record(record);
            self.0.insert(ukey, uval);
            Ok(SetStatus { cas: 0 })
        }
    }

    fn delete(
        &self,
        key: crate::memcache::store::KeyType,
        header: crate::cache::cache::CacheMetaData,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        let ukey = UnifiedStr::from_bytes(&key[..]);
        let mut cas_match: Option<bool> = None;
        match self.0.remove_if(&ukey, |_key, map_value| -> bool {
            let record = map_value.to_record();
            let result = header.cas == 0 || record.header.cas == header.cas;
            cas_match = Some(result);
            result
        }) {
            Some(key_value) => Ok(key_value.1.to_record()),
            None => match cas_match {
                Some(_value) => Err(CacheError::KeyExists),
                None => Err(CacheError::NotFound),
            },
        }
    }

    fn flush(&self, header: crate::cache::cache::CacheMetaData) {
        if header.time_to_live > 0 {
            self.0.alter_all(|_key, map_value| {
                let mut record = map_value.to_record();
                record.header.time_to_live = header.time_to_live;
                MapValue::from_record(record)
            });
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
            .iter()
            .filter(|entry: &RefMulti<UnifiedStr, MapValue, UnifiedStrHasher>| {
                let key_bytes = Bytes::copy_from_slice(entry.key().as_bytes_trimmed());
                let record = entry.value().to_record_ref();
                f(&key_bytes, record)
            })
            .map(|entry: RefMulti<UnifiedStr, MapValue, UnifiedStrHasher>| {
                Bytes::copy_from_slice(entry.key().as_bytes_trimmed())
            })
            .collect()
    }
}
