use super::{StorageBackend, cas_common::CasOperations};
use crate::{
    cache::{
        cache::{KeyType, Record, SetStatus},
        error::CacheError,
    },
    memory_store::store::Peripherals,
};

use flurry::HashMap;

pub struct FlurryMapBackend(HashMap<KeyType, Record>);

impl StorageBackend for FlurryMapBackend {
    fn init(cap: usize) -> Self {
        Self(HashMap::with_capacity(cap.next_power_of_two()))
    }

    fn get(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        let mref = self.0.pin();
        mref.get(key).map(|v| v.clone()).ok_or(CacheError::NotFound)
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        let mref = self.0.pin();
        mref.remove(key).map(|v| v.clone())
    }

    fn set(
        &self,
        key: crate::memcache::store::KeyType,
        mut record: crate::memcache::store::Record,
        peripherals: &Peripherals,
    ) -> crate::cache::error::Result<crate::cache::cache::SetStatus> {
        let mref = self.0.pin();
        
        let result = CasOperations::execute_set_operation(
            &mut record,
            peripherals,
            || {
                mref.get(&key).cloned()
            },
        )?;
        
        // Insert/update the record in the map
        mref.insert(key, record);
        
        Ok(result)
    }

    fn delete(
        &self,
        key: crate::memcache::store::KeyType,
        header: crate::cache::cache::CacheMetaData,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        let mref = self.0.pin();
        
        CasOperations::execute_delete_operation(
            &header,
            || {
                mref.get(&key).cloned()
            },
            || {
                mref.remove(&key).cloned()
            },
        )
    }

    fn flush(&self, header: crate::cache::cache::CacheMetaData) {
        let mref = self.0.pin();
        mref.clear();
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn predict_keys(
        &self,
        f: &mut crate::cache::cache::CachePredicate,
    ) -> Vec<crate::memcache::store::KeyType> {
        let mref = self.0.pin();
        mref.iter()
            .filter(|(k, v)| f(k, v))
            .map(|(k, v)| k.clone())
            .collect()
    }
}
