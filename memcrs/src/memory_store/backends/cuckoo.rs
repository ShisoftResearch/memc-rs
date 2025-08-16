use bytes::Bytes;
use lockfree_cuckoohash::*;

use crate::{cache::error::CacheError, memcache::store::*, memory_store::store::Peripherals};

use super::StorageBackend;

pub struct CuckooBackend(LockFreeCuckooHash<KeyType, Record>);

impl StorageBackend for CuckooBackend {
    fn init(cap: usize) -> Self {
        Self(LockFreeCuckooHash::with_capacity(cap.next_power_of_two()))
    }

    fn get(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        let g = pin();
        self.0
            .get(key, &g)
            .map(|v| v.clone())
            .ok_or(CacheError::NotFound)
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        if self.0.remove(key) {
            return Some(Record::new(Bytes::new(), 0, 0, 0));
        } else {
            return None;
        }
    }

    fn set(
        &self,
        key: crate::memcache::store::KeyType,
        mut record: crate::memcache::store::Record,
        peripherals: &Peripherals,
    ) -> crate::cache::error::Result<crate::cache::cache::SetStatus> {
        //trace!("Set: {:?}", &record.header);
        if record.header.cas > 0 {
            unimplemented!();
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
        unimplemented!()
    }

    fn flush(&self, header: crate::cache::cache::CacheMetaData) {
        unimplemented!()
    }

    fn len(&self) -> usize {
        self.0.size()
    }

    fn predict_keys(
        &self,
        f: &mut crate::cache::cache::CachePredicate,
    ) -> Vec<crate::memcache::store::KeyType> {
        unimplemented!()
    }
}
