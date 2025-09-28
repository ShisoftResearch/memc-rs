use super::{StorageBackend, cas_common::CasOperations};
use crate::{
    cache::{
        cache::{KeyType, Record, SetStatus},
        error::CacheError,
    },
    memory_store::store::Peripherals,
};
use scc::HashMap;

pub struct SccHashMapBackend(HashMap<KeyType, Record>);

impl StorageBackend for SccHashMapBackend {
    fn init(cap: usize) -> Self {
        Self(HashMap::with_capacity(cap.next_power_of_two()))
    }

    fn get(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        self.0
            .get_sync(key)
            .map(|v: scc::hash_map::OccupiedEntry<'_, bytes::Bytes, Record>| v.get().clone())
            .ok_or(CacheError::NotFound)
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        self.0.remove_sync(key).map(|(_, v)| v)
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
                self.0.get_sync(&key)
                    .map(|entry| entry.get().clone())
            },
        )?;
        
        // Insert/update the record in the map
        let _ = self.0.insert_sync(key, record);
        
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
                self.0.get_sync(&key)
                    .map(|entry| entry.get().clone())
            },
            || {
                self.0.remove_sync(&key).map(|(_, record)| record)
            },
        )
    }

    fn flush(&self, header: crate::cache::cache::CacheMetaData) {
        if header.time_to_live > 0 {
            // For scc, we can't easily implement selective flush based on TTL
            // So we just clear everything when flush is called
            self.0.clear_sync();
        } else {
            self.0.clear_sync();
        }
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn predict_keys(
        &self,
        f: &mut crate::cache::cache::CachePredicate,
    ) -> Vec<crate::memcache::store::KeyType> {
        // For scc, we can't easily iterate over all keys
        // Return empty vector as this is not commonly used
        Vec::new()
    }
}
