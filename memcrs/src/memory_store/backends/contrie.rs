use super::{StorageBackend, cas_common::CasOperations};
use crate::{cache::error::CacheError, memcache::store::*, memory_store::store::Peripherals};

use contrie::ConMap;

pub struct ContrieBackend(ConMap<KeyType, Record>);

impl StorageBackend for ContrieBackend {
    fn init(_cap: usize) -> Self {
        Self(ConMap::new())
    }

    fn get(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        self.0
            .get(key)
            .map(|v| v.value().clone())
            .ok_or(CacheError::NotFound)
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        self.0.remove(key).map(|e| e.value().clone())
    }

    fn set(
        &self,
        key: crate::memcache::store::KeyType,
        mut record: crate::memcache::store::Record,
        peripherals: &Peripherals,
    ) -> crate::cache::error::Result<crate::cache::cache::SetStatus> {
        let result = CasOperations::execute_set_operation(
            &mut record,
            peripherals,
            || {
                self.0.get(&key).map(|entry| entry.value().clone())
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
        CasOperations::execute_delete_operation(
            &header,
            || {
                self.0.get(&key).map(|entry| entry.value().clone())
            },
            || {
                self.0.remove(&key).map(|entry| entry.value().clone())
            },
        )
    }

    fn flush(&self, header: crate::cache::cache::CacheMetaData) {
        if header.time_to_live > 0 {
            // For contrie, we can't easily implement selective flush based on TTL
            // So we just clear everything when flush is called
            // ConMap doesn't have a clear method, so we need to remove all entries
            let keys: Vec<_> = self.0.iter().map(|entry| entry.key().clone()).collect();
            for key in keys {
                let _ = self.0.remove(&key);
            }
        } else {
            let keys: Vec<_> = self.0.iter().map(|entry| entry.key().clone()).collect();
            for key in keys {
                let _ = self.0.remove(&key);
            }
        }
    }

    fn len(&self) -> usize {
        self.0.iter().count()
    }

    fn predict_keys(
        &self,
        f: &mut crate::cache::cache::CachePredicate,
    ) -> Vec<crate::memcache::store::KeyType> {
        self.0
            .iter()
            .filter(|record| f(record.key(), record.value()))
            .map(|record| record.key().clone())
            .collect()
    }
}
