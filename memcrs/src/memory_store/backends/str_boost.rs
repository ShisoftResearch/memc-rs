use std::sync::Arc;

use crate::cache::error::CacheError;
use crate::{
    cache::cache::{CacheMetaData, CachePredicate, SetStatus},
    memcache::store::{KeyType, Record},
    memory_store::store::Peripherals,
};

use super::{StorageBackend, cas_common::CasOperations};
use crate::ffi::unified_str::{MapValue, UnifiedStr, MAP_VAL_BUFFER_CAP, UNIFIED_STR_CAP};

#[repr(C)]
pub struct BoostStringMapOpaque {
    _private: [u8; 0],
}

extern "C" {
    fn new_boost_string_map(capacity: usize) -> *mut BoostStringMapOpaque;
    fn free_boost_string_map(map: *mut BoostStringMapOpaque);
    fn boost_string_update(
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
            Ok(out.to_record_ref().clone())
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
        
        let result = CasOperations::execute_set_operation(
            &mut record,
            peripherals,
            || {
                let mut out = MapValue {
                    data: [0; MAP_VAL_BUFFER_CAP],
                };
                let found = unsafe { boost_string_get(*self.map, &ukey, &mut out as *mut MapValue) };
                if found {
                    Some(out.to_record_ref().clone())
                } else {
                    None
                }
            },
        )?;
        
        // Insert/update the record in the map
        let uval = MapValue::from_record(record);
        let _ = unsafe { boost_string_update(*self.map, &ukey, &uval) };
        
        Ok(result)
    }

    fn delete(&self, key: KeyType, header: CacheMetaData) -> crate::cache::error::Result<Record> {
        let ukey = bytes_to_unified_str(&key);
        
        CasOperations::execute_delete_operation(
            &header,
            || {
                let mut out = MapValue {
                    data: [0; MAP_VAL_BUFFER_CAP],
                };
                let found = unsafe { boost_string_get(*self.map, &ukey, &mut out as *mut MapValue) };
                if found {
                    Some(out.to_record_ref().clone())
                } else {
                    None
                }
            },
            || {
                let mut out = MapValue {
                    data: [0; MAP_VAL_BUFFER_CAP],
                };
                let found = unsafe { boost_string_get(*self.map, &ukey, &mut out as *mut MapValue) };
                if found {
                    let removed = unsafe { boost_string_remove(*self.map, &ukey) };
                    if removed {
                        Some(out.to_record())
                    } else {
                        None
                    }
                } else {
                    None
                }
            },
        )
    }

    fn flush(&self, _header: CacheMetaData) {}
    fn len(&self) -> usize {
        unsafe { boost_string_size(*self.map) as usize }
    }
    fn predict_keys(&self, _f: &mut CachePredicate) -> Vec<KeyType> {
        Vec::new()
    }
}
