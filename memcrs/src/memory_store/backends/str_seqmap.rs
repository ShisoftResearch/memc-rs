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
pub struct SeqStringMapOpaque;

extern "C" {
    fn new_seq_string_map(capacity: usize) -> *mut SeqStringMapOpaque;
    fn free_seq_string_map(map: *mut SeqStringMapOpaque);
    fn seq_string_find(
        map: *mut SeqStringMapOpaque,
        key: &UnifiedStr,
        out_value: *mut MapValue,
    ) -> bool;
    fn seq_string_insert(
        map: *mut SeqStringMapOpaque,
        key: &UnifiedStr,
        value: &MapValue,
    ) -> bool;
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
            let shadow = out.to_record();
            let val = shadow.clone();
            mem::forget(shadow);
            Ok(val)
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
        if record.header.cas > 0 {
            record.header.cas += 1;
            record.header.timestamp = peripherals.timestamp();
            let cas = record.header.cas;
            let uval = MapValue::from_record(record);
            let ok = unsafe { seq_string_update(*self.map, &ukey, &uval) };
            if ok {
                Ok(SetStatus {
                    cas,
                })
            } else {
                Err(CacheError::KeyExists)
            }
        } else {
            let uval = MapValue::from_record(record);
            let _ = unsafe { seq_string_update(*self.map, &ukey, &uval) };
            Ok(SetStatus { cas: 0 })
        }
    }
    fn delete(&self, key: KeyType, header: CacheMetaData) -> crate::cache::error::Result<Record> {
        let ukey = UnifiedStr::from_bytes(&key);
        let mut out = MapValue {
            data: [0; MAP_VAL_BUFFER_CAP],
        };
        if !unsafe { seq_string_find(*self.map, &ukey, &mut out as *mut MapValue) } {
            return Err(CacheError::NotFound);
        }
        if header.cas != 0 {
            return Err(CacheError::KeyExists);
        }
        if unsafe { seq_string_remove(*self.map, &ukey) } {
            let mut record = out.to_record();
            record.header = header;
            Ok(record)
        } else {
            Err(CacheError::NotFound)
        }
    }
    fn flush(&self, _header: CacheMetaData) {}
    fn len(&self) -> usize {
        unsafe { seq_string_size(*self.map) as usize }
    }
    fn predict_keys(&self, _f: &mut CachePredicate) -> Vec<KeyType> {
        Vec::new()
    }
}
