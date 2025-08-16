use std::sync::Arc;

use crate::cache::error::CacheError;
use crate::{
    cache::cache::{CacheMetaData, CachePredicate, SetStatus},
    memcache::store::{KeyType, Record},
    memory_store::store::Peripherals,
};

use super::StorageBackend;
use crate::ffi::unified_str::{
    UnifiedStr, MapValue, UNIFIED_STR_CAP, UNIFIED_STR_LARGE_CAP,
};

#[repr(C)]
pub struct TbbStringMapOpaque;

extern "C" {
    fn new_tbb_string_map(capacity: usize) -> *mut TbbStringMapOpaque;
    fn free_tbb_string_map(map: *mut TbbStringMapOpaque);
    fn tbb_string_insert(
        map: *mut TbbStringMapOpaque,
        key: &UnifiedStr,
        value: &MapValue,
    ) -> bool;
    fn tbb_string_get(
        map: *mut TbbStringMapOpaque,
        key: &UnifiedStr,
        out_value: *mut MapValue,
    ) -> bool;
    fn tbb_string_remove(map: *mut TbbStringMapOpaque, key: &UnifiedStr) -> bool;
    fn tbb_string_update(
        map: *mut TbbStringMapOpaque,
        key: &UnifiedStr,
        value: &MapValue,
    ) -> bool;
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

fn bytes_to_unified_str(buf: &KeyType) -> UnifiedStr {
    let mut data = [0u8; UNIFIED_STR_CAP];
    let len = core::cmp::min(buf.len(), UNIFIED_STR_CAP);
    data[..len].copy_from_slice(&buf[..len]);
    UnifiedStr { data }
}
fn bytes_to_unified_str_large(buf: &bytes::Bytes) -> MapValue {
    let mut data = [0u8; UNIFIED_STR_LARGE_CAP];
    let len = core::cmp::min(buf.len(), UNIFIED_STR_LARGE_CAP);
    data[..len].copy_from_slice(&buf[..len]);
    MapValue { data }
}

impl StorageBackend for TbbStringBackend {
    fn init(cap: usize) -> Self {
        let map = unsafe { new_tbb_string_map(cap) };
        Self { map: Arc::new(map) }
    }
    fn get(&self, key: &KeyType) -> crate::cache::error::Result<Record> {
        let ukey = bytes_to_unified_str(key);
        let mut out = MapValue {
            data: [0; UNIFIED_STR_LARGE_CAP],
        };
        if unsafe { tbb_string_get(*self.map, &ukey, &mut out as *mut MapValue) } {
            Ok(Record {
                header: CacheMetaData::new(0, 0, 0),
                value: bytes::Bytes::copy_from_slice(out.as_bytes_trimmed()),
            })
        } else {
            Err(CacheError::NotFound)
        }
    }
    fn remove(&self, key: &KeyType) -> Option<Record> {
        let ukey = bytes_to_unified_str(key);
        let mut out = MapValue {
            data: [0; UNIFIED_STR_LARGE_CAP],
        };
        if !unsafe { tbb_string_get(*self.map, &ukey, &mut out as *mut MapValue) } {
            return None;
        }
        if unsafe { tbb_string_remove(*self.map, &ukey) } {
            Some(Record {
                header: CacheMetaData::new(0, 0, 0),
                value: bytes::Bytes::copy_from_slice(out.as_bytes_trimmed()),
            })
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
        let uval = bytes_to_unified_str_large(&record.value);
        if record.header.cas > 0 {
            record.header.cas += 1;
            record.header.timestamp = peripherals.timestamp();
            let ok = unsafe { tbb_string_update(*self.map, &ukey, &uval) };
            if ok {
                Ok(SetStatus {
                    cas: record.header.cas,
                })
            } else {
                Err(CacheError::KeyExists)
            }
        } else {
            let _ = unsafe { tbb_string_insert(*self.map, &ukey, &uval) };
            Ok(SetStatus { cas: 0 })
        }
    }
    fn delete(&self, key: KeyType, header: CacheMetaData) -> crate::cache::error::Result<Record> {
        let ukey = bytes_to_unified_str(&key);
        let mut out = MapValue {
            data: [0; UNIFIED_STR_LARGE_CAP],
        };
        if !unsafe { tbb_string_get(*self.map, &ukey, &mut out as *mut MapValue) } {
            return Err(CacheError::NotFound);
        }
        if header.cas != 0 {
            return Err(CacheError::KeyExists);
        }
        if unsafe { tbb_string_remove(*self.map, &ukey) } {
            Ok(Record {
                header,
                value: bytes::Bytes::copy_from_slice(out.as_bytes_trimmed()),
            })
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
