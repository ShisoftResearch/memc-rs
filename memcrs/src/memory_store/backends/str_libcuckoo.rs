use std::sync::Arc;

use crate::cache::error::CacheError;
use crate::{
    cache::cache::{CacheMetaData, CachePredicate, SetStatus},
    memcache::store::{KeyType, Record},
    memory_store::store::Peripherals,
};

use super::StorageBackend;
use crate::ffi::unified_str::{
    UnifiedStr, UnifiedStrLarge, UNIFIED_STR_CAP, UNIFIED_STR_LARGE_CAP,
};

#[repr(C)]
pub struct CuckooStringMapOpaque;

extern "C" {
    fn new_cuckoo_string_map(capacity: usize) -> *mut CuckooStringMapOpaque;
    fn free_cuckoo_string_map(map: *mut CuckooStringMapOpaque);
    fn cuckoo_string_insert(
        map: *mut CuckooStringMapOpaque,
        key: &UnifiedStr,
        value: &UnifiedStrLarge,
    ) -> bool;
    fn cuckoo_string_get(
        map: *mut CuckooStringMapOpaque,
        key: &UnifiedStr,
        out_value: *mut UnifiedStrLarge,
    ) -> bool;
    fn cuckoo_string_remove(map: *mut CuckooStringMapOpaque, key: &UnifiedStr) -> bool;
    fn cuckoo_string_size(map: *mut CuckooStringMapOpaque) -> i64;
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

fn bytes_to_unified_str(buf: &KeyType) -> UnifiedStr {
    let mut data = [0u8; UNIFIED_STR_CAP];
    let len = core::cmp::min(buf.len(), UNIFIED_STR_CAP);
    data[..len].copy_from_slice(&buf[..len]);
    UnifiedStr { data }
}
fn bytes_to_unified_str_large(buf: &[u8]) -> UnifiedStrLarge {
    let mut data = [0u8; UNIFIED_STR_LARGE_CAP];
    let len = core::cmp::min(buf.len(), UNIFIED_STR_LARGE_CAP);
    data[..len].copy_from_slice(&buf[..len]);
    UnifiedStrLarge { data }
}

impl StorageBackend for LibcuckooStringBackend {
    fn init(cap: usize) -> Self {
        let map = unsafe { new_cuckoo_string_map(cap) };
        Self { map: Arc::new(map) }
    }
    fn get(&self, key: &KeyType) -> crate::cache::error::Result<Record> {
        let ukey = bytes_to_unified_str(key);
        let mut out = UnifiedStrLarge {
            data: [0; UNIFIED_STR_LARGE_CAP],
        };
        if unsafe { cuckoo_string_get(*self.map, &ukey, &mut out as *mut UnifiedStrLarge) } {
            Ok(Record {
                header: CacheMetaData::new(0, 0, 0),
                value: out.as_bytes_trimmed().to_vec(),
            })
        } else {
            Err(CacheError::NotFound)
        }
    }
    fn remove(&self, key: &KeyType) -> Option<Record> {
        let ukey = bytes_to_unified_str(key);
        let mut out = UnifiedStrLarge {
            data: [0; UNIFIED_STR_LARGE_CAP],
        };
        if !unsafe { cuckoo_string_get(*self.map, &ukey, &mut out as *mut UnifiedStrLarge) } {
            return None;
        }
        if unsafe { cuckoo_string_remove(*self.map, &ukey) } {
            Some(Record {
                header: CacheMetaData::new(0, 0, 0),
                value: out.as_bytes_trimmed().to_vec(),
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
            let ok = unsafe { cuckoo_string_insert(*self.map, &ukey, &uval) };
            if ok {
                Ok(SetStatus {
                    cas: record.header.cas,
                })
            } else {
                Err(CacheError::KeyExists)
            }
        } else {
            let _ = unsafe { cuckoo_string_insert(*self.map, &ukey, &uval) };
            Ok(SetStatus { cas: 0 })
        }
    }
    fn delete(&self, key: KeyType, header: CacheMetaData) -> crate::cache::error::Result<Record> {
        let ukey = bytes_to_unified_str(&key);
        let mut out = UnifiedStrLarge {
            data: [0; UNIFIED_STR_LARGE_CAP],
        };
        if !unsafe { cuckoo_string_get(*self.map, &ukey, &mut out as *mut UnifiedStrLarge) } {
            return Err(CacheError::NotFound);
        }
        if header.cas != 0 {
            return Err(CacheError::KeyExists);
        }
        if unsafe { cuckoo_string_remove(*self.map, &ukey) } {
            Ok(Record {
                header,
                value: out.as_bytes_trimmed().to_vec(),
            })
        } else {
            Err(CacheError::NotFound)
        }
    }
    fn flush(&self, _header: CacheMetaData) {}
    fn len(&self) -> usize {
        unsafe { cuckoo_string_size(*self.map) as usize }
    }
    fn predict_keys(&self, _f: &mut CachePredicate) -> Vec<KeyType> {
        Vec::new()
    }
}
