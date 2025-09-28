use bytes::Bytes;
use lockfree_cuckoohash::*;

use crate::{cache::error::CacheError, memcache::store::*, memory_store::store::Peripherals};

use super::{StorageBackend, cas_common::CasOperations};

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
        let g = pin();
        
        let result = CasOperations::execute_set_operation(
            &mut record,
            peripherals,
            || {
                self.0.get(&key, &g).cloned()
            },
        )?;
        
        // Insert/update the record in the map
        self.0.insert(key, record);
        
        Ok(result)
    }

    fn delete(
        &self,
        key: crate::memcache::store::KeyType,
        header: crate::cache::cache::CacheMetaData,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        let g = pin();
        
        CasOperations::execute_delete_operation(
            &header,
            || {
                self.0.get(&key, &g).cloned()
            },
            || {
                if self.0.remove(&key) {
                    Some(Record::new(Bytes::new(), 0, 0, 0))
                } else {
                    None
                }
            },
        )
    }

    fn flush(&self, _header: crate::cache::cache::CacheMetaData) {
        // For cuckoo hash, we can't easily implement selective flush based on TTL
        // So we just clear everything when flush is called
        unsafe {
            self.0.clear();
        }
    }

    fn len(&self) -> usize {
        self.0.size()
    }

    fn predict_keys(
        &self,
        f: &mut crate::cache::cache::CachePredicate,
    ) -> Vec<crate::memcache::store::KeyType> {
        // For cuckoo hash, we can't easily iterate over all keys
        // Return empty vector as this is not commonly used
        Vec::new()
    }
}
