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
pub struct CuckooStringMapOpaque {
    _private: [u8; 0],
}

extern "C" {
    fn new_cuckoo_string_map(capacity: usize) -> *mut CuckooStringMapOpaque;
    fn free_cuckoo_string_map(map: *mut CuckooStringMapOpaque);
    fn cuckoo_string_insert(
        map: *mut CuckooStringMapOpaque,
        key: &UnifiedStr,
        value: &MapValue,
    ) -> bool;
    fn cuckoo_string_get(
        map: *mut CuckooStringMapOpaque,
        key: &UnifiedStr,
        out_value: *mut MapValue,
    ) -> bool;
    fn cuckoo_string_remove(map: *mut CuckooStringMapOpaque, key: &UnifiedStr) -> bool;
    fn cuckoo_string_size(map: *mut CuckooStringMapOpaque) -> i64;
    fn cuckoo_string_update(
        map: *mut CuckooStringMapOpaque,
        key: &UnifiedStr,
        value: &MapValue,
    ) -> bool;
}

pub struct LibcuckooStringBackend {
    map: Arc<*mut CuckooStringMapOpaque>,
}

unsafe impl Send for LibcuckooStringBackend {}
unsafe impl Sync for LibcuckooStringBackend {}

impl Drop for LibcuckooStringBackend {
    fn drop(&mut self) {
        if Arc::strong_count(&self.map) == 1 {
            unsafe { free_cuckoo_string_map(*self.map) };
        }
    }
}

impl StorageBackend for LibcuckooStringBackend {
    fn init(cap: usize) -> Self {
        let map = unsafe { new_cuckoo_string_map(cap) };
        Self { map: Arc::new(map) }
    }
    fn get(&self, key: &KeyType) -> crate::cache::error::Result<Record> {
        let ukey = UnifiedStr::from_bytes(key);
        let mut out = MapValue {
            data: [0; MAP_VAL_BUFFER_CAP],
        };
        if unsafe { cuckoo_string_get(*self.map, &ukey, &mut out as *mut MapValue) } {
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
        if !unsafe { cuckoo_string_get(*self.map, &ukey, &mut out as *mut MapValue) } {
            return None;
        }
        if unsafe { cuckoo_string_remove(*self.map, &ukey) } {
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
                let found = unsafe { cuckoo_string_get(*self.map, &ukey, &mut out as *mut MapValue) };
                if found {
                    Some(out.to_record_ref().clone())
                } else {
                    None
                }
            },
        )?;
        
        // Insert/update the record in the map
        let uval = MapValue::from_record(record);
        let _ = unsafe { cuckoo_string_update(*self.map, &ukey, &uval) };
        
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
                let found = unsafe { cuckoo_string_get(*self.map, &ukey, &mut out as *mut MapValue) };
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
                let found = unsafe { cuckoo_string_get(*self.map, &ukey, &mut out as *mut MapValue) };
                if found {
                    let removed = unsafe { cuckoo_string_remove(*self.map, &ukey) };
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
        unsafe { cuckoo_string_size(*self.map) as usize }
    }
    fn predict_keys(&self, _f: &mut CachePredicate) -> Vec<KeyType> {
        Vec::new()
    }
}
