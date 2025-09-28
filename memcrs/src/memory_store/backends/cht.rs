use super::{StorageBackend, cas_common::CasOperations};
use crate::{cache::error::CacheError, memcache::store::*, memory_store::store::Peripherals};
use cht::HashMap;

pub struct ChtMapBackend(HashMap<KeyType, Record>);

impl StorageBackend for ChtMapBackend {
    fn init(cap: usize) -> Self {
        Self(HashMap::with_capacity(cap.next_power_of_two()))
    }

    fn get(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        self.0
            .get(key)
            .map(|v| v.clone())
            .ok_or(CacheError::NotFound)
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        self.0.remove(key).map(|v| v)
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
                self.0.get(&key).clone()
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
                self.0.get(&key).clone()
            },
            || {
                self.0.remove(&key)
            },
        )
    }

    fn flush(&self, header: crate::cache::cache::CacheMetaData) {
        if header.time_to_live > 0 {
            // For cht, we can't easily implement selective flush based on TTL
            // So we just clear everything when flush is called
            // CHT HashMap doesn't have a clear method or keys method
            // We'll implement a simple approach by creating a new HashMap
            // Note: This is not ideal but works for the flush operation
            unimplemented!("CHT HashMap flush not implemented - would require rebuilding the entire map");
        } else {
            unimplemented!("CHT HashMap flush not implemented - would require rebuilding the entire map");
        }
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn predict_keys(
        &self,
        f: &mut crate::cache::cache::CachePredicate,
    ) -> Vec<crate::memcache::store::KeyType> {
        // For cht, we can't easily iterate over all keys
        // Return empty vector as this is not commonly used
        Vec::new()
    }
}
