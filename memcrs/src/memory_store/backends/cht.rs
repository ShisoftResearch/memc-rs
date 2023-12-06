use cht::HashMap;
use crate::{memcache::store::*, cache::error::CacheError, memory_store::store::Peripherals};
use super::StorageBackend;

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
        //trace!("Set: {:?}", &record.header);
        if record.header.cas > 0 {
            unimplemented!()
        } else {
            let cas = peripherals.get_cas_id();
            record.header.cas = cas;
            record.header.timestamp = peripherals.timestamp();
            self.0.insert(key, record);
            Ok(SetStatus { cas })
        }
    }

    fn delete(
        &self,
        key: crate::memcache::store::KeyType,
        header: crate::cache::cache::CacheMetaData,
    ) -> crate::cache::error::Result<crate::memcache::store::Record> {
        let mut cas_match: Option<bool> = None;
        match self.0.remove_if(&key, |_key, record| -> bool {
            let result = header.cas == 0 || record.header.cas == header.cas;
            cas_match = Some(result);
            result
        }) {
            Some(key_value) => Ok(key_value),
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
            unimplemented!()
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
