use super::StorageBackend;
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
            .get(key)
            .map(|v: scc::hash_map::OccupiedEntry<'_, Vec<u8>, Record>| v.get().clone())
            .ok_or(CacheError::NotFound)
    }

    fn remove(
        &self,
        key: &crate::memcache::store::KeyType,
    ) -> Option<crate::memcache::store::Record> {
        self.0.remove(key).map(|(_, v)| v)
    }

    fn set(
        &self,
        key: crate::memcache::store::KeyType,
        mut record: crate::memcache::store::Record,
        peripherals: &Peripherals,
    ) -> crate::cache::error::Result<crate::cache::cache::SetStatus> {
        //trace!("Set: {:?}", &record.header);
        if record.header.cas > 0 {
            match self.0.get(&key) {
                Some(mut key_value_entry) => {
                    let key_value = key_value_entry.get_mut();
                    if key_value.header.cas != record.header.cas {
                        Err(CacheError::KeyExists)
                    } else {
                        record.header.cas += 1;
                        record.header.timestamp = peripherals.timestamp();
                        let cas = record.header.cas;
                        *key_value = record;
                        Ok(SetStatus { cas })
                    }
                }
                None => {
                    record.header.cas += 1;
                    record.header.timestamp = peripherals.timestamp();
                    let cas = record.header.cas;
                    self.0.insert(key, record);
                    Ok(SetStatus { cas })
                }
            }
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
        let mut cas_match: Option<bool> = None;
        match self.0.remove_if(&key, |record| -> bool {
            let result = header.cas == 0 || record.header.cas == header.cas;
            cas_match = Some(result);
            result
        }) {
            Some(key_value) => Ok(key_value.1),
            None => match cas_match {
                Some(_value) => Err(CacheError::KeyExists),
                None => Err(CacheError::NotFound),
            },
        }
    }

    fn flush(&self, header: crate::cache::cache::CacheMetaData) {
        if header.time_to_live > 0 {
            unimplemented!()
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
        unimplemented!()
    }
}
