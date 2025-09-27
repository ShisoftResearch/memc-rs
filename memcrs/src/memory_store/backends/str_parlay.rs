use std::mem;
use std::sync::Arc;

use crate::cache::error::CacheError;
use crate::{
    cache::cache::{CacheMetaData, CachePredicate, SetStatus},
    memcache::store::{KeyType, Record},
    memory_store::store::Peripherals,
};

use super::StorageBackend;
use crate::ffi::unified_str::{MapValue, UnifiedStr, MAP_VAL_BUFFER_CAP, UNIFIED_STR_CAP};

#[repr(C)]
pub struct ParlayStringMapOpaque {
    _private: [u8; 0],
}

extern "C" {
    fn new_string_map(capacity: usize) -> *mut ParlayStringMapOpaque;
    fn free_string_map(map: *mut ParlayStringMapOpaque);
    fn get_string_kv(
        map: *mut ParlayStringMapOpaque,
        key: &UnifiedStr,
        out_value: *mut MapValue,
    ) -> bool;
    fn remove_string_kv(map: *mut ParlayStringMapOpaque, key: &UnifiedStr) -> bool;
    fn update_string_kv(
        map: *mut ParlayStringMapOpaque,
        key: &UnifiedStr,
        value: &MapValue,
    ) -> bool;
}

pub struct ParlayStringBackend {
    map: Arc<*mut ParlayStringMapOpaque>,
}

unsafe impl Send for ParlayStringBackend {}
unsafe impl Sync for ParlayStringBackend {}

impl Drop for ParlayStringBackend {
    fn drop(&mut self) {
        if Arc::strong_count(&self.map) == 1 {
            unsafe { free_string_map(*self.map) };
        }
    }
}

impl StorageBackend for ParlayStringBackend {
    fn init(cap: usize) -> Self {
        let map = unsafe { new_string_map(cap) };
        Self { map: Arc::new(map) }
    }
    fn get(&self, key: &KeyType) -> crate::cache::error::Result<Record> {
        let ukey = UnifiedStr::from_bytes(key);
        let mut out = MapValue {
            data: [0; MAP_VAL_BUFFER_CAP],
        };
        if unsafe { get_string_kv(*self.map, &ukey, &mut out as *mut MapValue) } {
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
        if !unsafe { get_string_kv(*self.map, &ukey, &mut out as *mut MapValue) } {
            return None;
        }
        if unsafe { remove_string_kv(*self.map, &ukey) } {
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
        if record.header.cas > 0 {
            record.header.cas += 1;
            record.header.timestamp = peripherals.timestamp();
            let cas = record.header.cas;
            let uval = MapValue::from_record(record);
            let ok = unsafe { update_string_kv(*self.map, &ukey, &uval) };
            if ok {
                Ok(SetStatus { cas })
            } else {
                Err(CacheError::KeyExists)
            }
        } else {
            let uval = MapValue::from_record(record);
            let _ = unsafe { update_string_kv(*self.map, &ukey, &uval) };
            Ok(SetStatus { cas: 0 })
        }
    }
    fn delete(&self, key: KeyType, header: CacheMetaData) -> crate::cache::error::Result<Record> {
        let ukey = UnifiedStr::from_bytes(&key);
        let mut out = MapValue {
            data: [0; MAP_VAL_BUFFER_CAP],
        };
        if !unsafe { get_string_kv(*self.map, &ukey, &mut out as *mut MapValue) } {
            return Err(CacheError::NotFound);
        }
        if header.cas != 0 {
            return Err(CacheError::KeyExists);
        }
        if unsafe { remove_string_kv(*self.map, &ukey) } {
            let mut record = out.to_record();
            record.header = header;
            Ok(record)
        } else {
            Err(CacheError::NotFound)
        }
    }
    fn flush(&self, _header: CacheMetaData) {}
    fn len(&self) -> usize {
        0
    }
    fn predict_keys(&self, _f: &mut CachePredicate) -> Vec<KeyType> {
        Vec::new()
    }
}
