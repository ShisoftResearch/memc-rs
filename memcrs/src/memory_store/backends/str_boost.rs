use std::mem;
use std::sync::Arc;

use crate::cache::error::CacheError;
use crate::{
    cache::cache::{CacheMetaData, CachePredicate, SetStatus},
    memcache::store::{KeyType, Record},
    memory_store::store::Peripherals,
};

use super::StorageBackend;
use crate::ffi::unified_str::{
    UnifiedStr, MapValue, UNIFIED_STR_CAP, MAP_VAL_BUFFER_CAP,
};

#[repr(C)]
pub struct BoostStringMapOpaque;

extern "C" {
    fn new_boost_string_map(capacity: usize) -> *mut BoostStringMapOpaque;
    fn free_boost_string_map(map: *mut BoostStringMapOpaque);
    fn boost_string_insert(
        map: *mut BoostStringMapOpaque,
        key: &UnifiedStr,
        value: &MapValue,
    ) -> bool;
    fn boost_string_get(
        map: *mut BoostStringMapOpaque,
        key: &UnifiedStr,
        out_value: *mut MapValue,
    ) -> bool;
    fn boost_string_remove(map: *mut BoostStringMapOpaque, key: &UnifiedStr) -> bool;
    fn boost_string_size(map: *mut BoostStringMapOpaque) -> i64;
}

pub struct BoostStringBackend {
    map: Arc<*mut BoostStringMapOpaque>,
}

unsafe impl Send for BoostStringBackend {}
unsafe impl Sync for BoostStringBackend {}

impl Drop for BoostStringBackend {
    fn drop(&mut self) {
        if Arc::strong_count(&self.map) == 1 {
            unsafe { free_boost_string_map(*self.map) };
        }
    }
}

fn bytes_to_unified_str(buf: &KeyType) -> UnifiedStr {
    let mut data = [0u8; UNIFIED_STR_CAP];
    let len = core::cmp::min(buf.len(), UNIFIED_STR_CAP);
    data[..len].copy_from_slice(&buf[..len]);
    UnifiedStr { data }
}

impl StorageBackend for BoostStringBackend {
    fn init(cap: usize) -> Self {
        let map = unsafe { new_boost_string_map(cap) };
        Self { map: Arc::new(map) }
    }

    fn get(&self, key: &KeyType) -> crate::cache::error::Result<Record> {
        let ukey = bytes_to_unified_str(key);
        let mut out = MapValue {
            data: [0; MAP_VAL_BUFFER_CAP],
        };
        let found = unsafe { boost_string_get(*self.map, &ukey, &mut out as *mut MapValue) };
        if found {
            let shadow = out.to_record();
            let val = shadow.clone();
            mem::forget(shadow);
            Ok(val)
        } else {
            Err(CacheError::NotFound)
        }
    }

    fn remove(&self, key: &KeyType) -> Option<Record> {
        let ukey = bytes_to_unified_str(key);
        let mut out = MapValue {
            data: [0; MAP_VAL_BUFFER_CAP],
        };
        let found = unsafe { boost_string_get(*self.map, &ukey, &mut out as *mut MapValue) };
        if !found {
            return None;
        }
        let removed = unsafe { boost_string_remove(*self.map, &ukey) };
        if removed {
            Some(out.to_record())
        } else {
            None
        }
    }

    fn set(
        &self,
        key: KeyType,
        mut record: Record,
        peripherals: &Peripherals,
    ) -> crate::cache::error::Result<SetStatus> {
        let ukey = bytes_to_unified_str(&key);
        if record.header.cas > 0 {
            record.header.cas += 1;
            record.header.timestamp = peripherals.timestamp();
            let cas = record.header.cas;
            let uval = MapValue::from_record(record);
            let ok = unsafe { boost_string_insert(*self.map, &ukey, &uval) };
            if ok {
                Ok(SetStatus {
                    cas,
                })
            } else {
                Err(CacheError::KeyExists)
            }
        } else {
            let uval = MapValue::from_record(record);
            let _ = unsafe { boost_string_insert(*self.map, &ukey, &uval) };
            Ok(SetStatus { cas: 0 })
        }
    }

    fn delete(&self, key: KeyType, header: CacheMetaData) -> crate::cache::error::Result<Record> {
        let ukey = bytes_to_unified_str(&key);
        let mut out = MapValue {
            data: [0; MAP_VAL_BUFFER_CAP],
        };
        let found = unsafe { boost_string_get(*self.map, &ukey, &mut out as *mut MapValue) };
        if !found {
            return Err(CacheError::NotFound);
        }
        if header.cas != 0 {
            return Err(CacheError::KeyExists);
        }
        let removed = unsafe { boost_string_remove(*self.map, &ukey) };
        if removed {
            let mut record = out.to_record();
            record.header = header;
            Ok(record)
        } else {
            Err(CacheError::NotFound)
        }
    }

    fn flush(&self, _header: CacheMetaData) {}
    fn len(&self) -> usize {
        unsafe { boost_string_size(*self.map) as usize }
    }
    fn predict_keys(&self, _f: &mut CachePredicate) -> Vec<KeyType> {
        Vec::new()
    }
}
