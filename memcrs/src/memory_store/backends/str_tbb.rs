use std::sync::Arc;

use crate::cache::error::CacheError;
use crate::{
    cache::cache::{CacheMetaData, CachePredicate, SetStatus},
    memcache::store::{KeyType, Record},
    memory_store::store::Peripherals,
};

use super::{StorageBackend, cas_common::CasOperations};
use crate::ffi::unified_str::{MapValue, UnifiedStr, MAP_VAL_BUFFER_CAP};

#[repr(C)]
pub struct TbbStringMapOpaque {
    _private: [u8; 0],
}

extern "C" {
    fn new_tbb_string_map(capacity: usize) -> *mut TbbStringMapOpaque;
    fn free_tbb_string_map(map: *mut TbbStringMapOpaque);
    fn tbb_string_insert(map: *mut TbbStringMapOpaque, key: &UnifiedStr, value: &MapValue) -> bool;
    fn tbb_string_get(
        map: *mut TbbStringMapOpaque,
        key: &UnifiedStr,
        out_value: *mut MapValue,
    ) -> bool;
    fn tbb_string_remove(map: *mut TbbStringMapOpaque, key: &UnifiedStr) -> bool;
    fn tbb_string_update(map: *mut TbbStringMapOpaque, key: &UnifiedStr, value: &MapValue) -> bool;
}

pub struct TbbStringBackend {
    map: Arc<*mut TbbStringMapOpaque>,
}

unsafe impl Send for TbbStringBackend {}
unsafe impl Sync for TbbStringBackend {}

impl Drop for TbbStringBackend {
    fn drop(&mut self) {
        if Arc::strong_count(&self.map) == 1 {
            unsafe { free_tbb_string_map(*self.map) };
        }
    }
}

impl StorageBackend for TbbStringBackend {
    fn init(cap: usize) -> Self {
        let map = unsafe { new_tbb_string_map(cap) };
        Self { map: Arc::new(map) }
    }
    fn get(&self, key: &KeyType) -> crate::cache::error::Result<Record> {
        let ukey = UnifiedStr::from_bytes(key);
        let mut out = MapValue {
            data: [0; MAP_VAL_BUFFER_CAP],
        };
        if unsafe { tbb_string_get(*self.map, &ukey, &mut out as *mut MapValue) } {
            Ok(out.to_record_ref().clone())
        } else {
            Err(CacheError::NotFound)
        }
    }
    fn remove(&self, key: &KeyType) -> Option<Record> {
        let ukey = UnifiedStr::from_bytes(key);
        let mut out = MapValue {
            data: [0; MAP_VAL_BUFFER_CAP],
        };
        if !unsafe { tbb_string_get(*self.map, &ukey, &mut out as *mut MapValue) } {
            return None;
        }
        if unsafe { tbb_string_remove(*self.map, &ukey) } {
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
        let ukey = UnifiedStr::from_bytes(&key);
        
        let result = CasOperations::execute_set_operation(
            &mut record,
            peripherals,
            || {
                let mut out = MapValue {
                    data: [0; MAP_VAL_BUFFER_CAP],
                };
                let found = unsafe { tbb_string_get(*self.map, &ukey, &mut out as *mut MapValue) };
                if found {
                    Some(out.to_record_ref().clone())
                } else {
                    None
                }
            },
        )?;
        
        // Insert/update the record in the map
        let uval = MapValue::from_record(record);
        let _ = unsafe { tbb_string_update(*self.map, &ukey, &uval) };
        
        Ok(result)
    }
    fn delete(&self, key: KeyType, header: CacheMetaData) -> crate::cache::error::Result<Record> {
        let ukey = UnifiedStr::from_bytes(&key);
        
        CasOperations::execute_delete_operation(
            &header,
            || {
                let mut out = MapValue {
                    data: [0; MAP_VAL_BUFFER_CAP],
                };
                let found = unsafe { tbb_string_get(*self.map, &ukey, &mut out as *mut MapValue) };
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
                let found = unsafe { tbb_string_get(*self.map, &ukey, &mut out as *mut MapValue) };
                if found {
                    let removed = unsafe { tbb_string_remove(*self.map, &ukey) };
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
        0
    }
    fn predict_keys(&self, _f: &mut CachePredicate) -> Vec<KeyType> {
        Vec::new()
    }
}
