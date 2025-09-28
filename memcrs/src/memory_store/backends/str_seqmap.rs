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
pub struct SeqStringMapOpaque {
    _private: [u8; 0],
}

extern "C" {
    fn new_seq_string_map(capacity: usize) -> *mut SeqStringMapOpaque;
    fn free_seq_string_map(map: *mut SeqStringMapOpaque);
    fn seq_string_find(
        map: *mut SeqStringMapOpaque,
        key: &UnifiedStr,
        out_value: *mut MapValue,
    ) -> bool;
    fn seq_string_insert(map: *mut SeqStringMapOpaque, key: &UnifiedStr, value: &MapValue) -> bool;
    fn seq_string_remove(map: *mut SeqStringMapOpaque, key: &UnifiedStr) -> bool;
    fn seq_string_size(map: *mut SeqStringMapOpaque) -> i64;
    fn seq_string_update(map: *mut SeqStringMapOpaque, key: &UnifiedStr, value: &MapValue) -> bool;
}

pub struct SeqStringBackend {
    map: Arc<*mut SeqStringMapOpaque>,
}

unsafe impl Send for SeqStringBackend {}
unsafe impl Sync for SeqStringBackend {}

impl Drop for SeqStringBackend {
    fn drop(&mut self) {
        if Arc::strong_count(&self.map) == 1 {
            unsafe { free_seq_string_map(*self.map) };
        }
    }
}

impl StorageBackend for SeqStringBackend {
    fn init(cap: usize) -> Self {
        let map = unsafe { new_seq_string_map(cap) };
        Self { map: Arc::new(map) }
    }
    fn get(&self, key: &KeyType) -> crate::cache::error::Result<Record> {
        let ukey = UnifiedStr::from_bytes(key);
        let mut out = MapValue {
            data: [0; MAP_VAL_BUFFER_CAP],
        };
        if unsafe { seq_string_find(*self.map, &ukey, &mut out as *mut MapValue) } {
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
        if !unsafe { seq_string_find(*self.map, &ukey, &mut out as *mut MapValue) } {
            return None;
        }
        if unsafe { seq_string_remove(*self.map, &ukey) } {
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
                let found = unsafe { seq_string_find(*self.map, &ukey, &mut out as *mut MapValue) };
                if found {
                    Some(out.to_record_ref().clone())
                } else {
                    None
                }
            },
        )?;
        
        // Insert/update the record in the map
        let uval = MapValue::from_record(record);
        let _ = unsafe { seq_string_update(*self.map, &ukey, &uval) };
        
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
                let found = unsafe { seq_string_find(*self.map, &ukey, &mut out as *mut MapValue) };
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
                let found = unsafe { seq_string_find(*self.map, &ukey, &mut out as *mut MapValue) };
                if found {
                    let removed = unsafe { seq_string_remove(*self.map, &ukey) };
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
        unsafe { seq_string_size(*self.map) as usize }
    }
    fn predict_keys(&self, _f: &mut CachePredicate) -> Vec<KeyType> {
        Vec::new()
    }
}
