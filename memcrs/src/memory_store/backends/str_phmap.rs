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
pub struct ParallelStringMapOpaque;

extern "C" {
    fn new_parallel_string_map(capacity: usize) -> *mut ParallelStringMapOpaque;
    fn free_parallel_string_map(map: *mut ParallelStringMapOpaque);
    fn parallel_string_insert(
        map: *mut ParallelStringMapOpaque,
        key: &UnifiedStr,
        value: &UnifiedStrLarge,
    ) -> bool;
    fn parallel_string_get(
        map: *mut ParallelStringMapOpaque,
        key: &UnifiedStr,
        out_value: *mut UnifiedStrLarge,
    ) -> bool;
    fn parallel_string_remove(map: *mut ParallelStringMapOpaque, key: &UnifiedStr) -> bool;
    fn parallel_string_size(map: *mut ParallelStringMapOpaque) -> i64;
}

pub struct PhmapStringBackend {
    map: Arc<*mut ParallelStringMapOpaque>,
}

unsafe impl Send for PhmapStringBackend {}
unsafe impl Sync for PhmapStringBackend {}

impl Drop for PhmapStringBackend {
    fn drop(&mut self) {
        if Arc::strong_count(&self.map) == 1 {
            unsafe { free_parallel_string_map(*self.map) };
        }
    }
}

fn bytes_to_unified_str(buf: &KeyType) -> UnifiedStr {
    let mut data = [0u8; UNIFIED_STR_CAP];
    let len = core::cmp::min(buf.len(), UNIFIED_STR_CAP);
    data[..len].copy_from_slice(&buf[..len]);
    UnifiedStr { data }
}

fn bytes_to_unified_str_large(buf: &bytes::Bytes) -> UnifiedStrLarge {
    let mut data = [0u8; UNIFIED_STR_LARGE_CAP];
    let len = core::cmp::min(buf.len(), UNIFIED_STR_LARGE_CAP);
    data[..len].copy_from_slice(&buf[..len]);
    UnifiedStrLarge { data }
}

impl StorageBackend for PhmapStringBackend {
    fn init(cap: usize) -> Self {
        let map = unsafe { new_parallel_string_map(cap) };
        Self { map: Arc::new(map) }
    }

    fn get(&self, key: &KeyType) -> crate::cache::error::Result<Record> {
        let ukey = bytes_to_unified_str(key);
        let mut out = UnifiedStrLarge {
            data: [0; UNIFIED_STR_LARGE_CAP],
        };
        let found =
            unsafe { parallel_string_get(*self.map, &ukey, &mut out as *mut UnifiedStrLarge) };
        if found {
            Ok(Record {
                header: CacheMetaData::new(0, 0, 0),
                value: bytes::Bytes::copy_from_slice(&out.data),
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
        let found =
            unsafe { parallel_string_get(*self.map, &ukey, &mut out as *mut UnifiedStrLarge) };
        if !found {
            return None;
        }
        let removed = unsafe { parallel_string_remove(*self.map, &ukey) };
        if removed {
            Some(Record {
                header: CacheMetaData::new(0, 0, 0),
                value: bytes::Bytes::copy_from_slice(&out.data),
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
            // Emulate CAS: get existing value and only insert if cas matches; we cannot read CAS from FFI store
            // For benchmarking, just bump CAS and write.
            record.header.cas += 1;
            record.header.timestamp = peripherals.timestamp();
            let ok = unsafe { parallel_string_insert(*self.map, &ukey, &uval) };
            if ok {
                Ok(SetStatus {
                    cas: record.header.cas,
                })
            } else {
                Err(CacheError::KeyExists)
            }
        } else {
            let ok = unsafe { parallel_string_insert(*self.map, &ukey, &uval) };
            if ok {
                Ok(SetStatus { cas: 0 })
            } else {
                Ok(SetStatus { cas: 0 })
            }
        }
    }

    fn delete(&self, key: KeyType, header: CacheMetaData) -> crate::cache::error::Result<Record> {
        let ukey = bytes_to_unified_str(&key);
        let mut out = UnifiedStrLarge {
            data: [0; UNIFIED_STR_LARGE_CAP],
        };
        let found =
            unsafe { parallel_string_get(*self.map, &ukey, &mut out as *mut UnifiedStrLarge) };
        if !found {
            return Err(CacheError::NotFound);
        }
        let matching = header.cas == 0; // Emulate CAS semantics only partially
        if !matching {
            return Err(CacheError::KeyExists);
        }
        let removed = unsafe { parallel_string_remove(*self.map, &ukey) };
        if removed {
            Ok(Record {
                header,
                value: bytes::Bytes::copy_from_slice(&out.data),
            })
        } else {
            Err(CacheError::NotFound)
        }
    }

    fn flush(&self, _header: CacheMetaData) { /* not supported */
    }

    fn len(&self) -> usize {
        unsafe { parallel_string_size(*self.map) as usize }
    }

    fn predict_keys(&self, _f: &mut CachePredicate) -> Vec<KeyType> {
        Vec::new()
    }
}
