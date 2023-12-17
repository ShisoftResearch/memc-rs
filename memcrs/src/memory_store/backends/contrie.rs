use super::StorageBackend;
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
        //trace!("Set: {:?}", &record.header);
        if record.header.cas > 0 {
            record.header.cas += 1;
            record.header.timestamp = peripherals.timestamp();
            let cas = record.header.cas;
            self.0.insert(key, record);
            Ok(SetStatus { cas })
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
        if let Some(e) = self.0.get(&key) {
            let record = e.value();
            let matching = header.cas == 0 || record.header.cas == header.cas;
            if matching {
                return self.remove(&key).ok_or(CacheError::NotFound);
            } else {
                return Err(CacheError::KeyExists);
            }
        } else {
            return Err(CacheError::NotFound);
        }
    }

    fn flush(&self, header: crate::cache::cache::CacheMetaData) {
        unimplemented!()
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
