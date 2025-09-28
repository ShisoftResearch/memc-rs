use bytes::{Bytes, BytesMut};

use crate::cache::cache::{
    Cache, CacheMetaData as CacheMeta, KeyType as CacheKeyType, Record as CacheRecord,
    SetStatus as CacheSetStatus,
};
use crate::cache::error::{CacheError, Result};

use std::str;
use std::sync::Arc;

pub type Record = CacheRecord;
pub type Meta = CacheMeta;
pub type SetStatus = CacheSetStatus;
pub type KeyType = CacheKeyType;

#[derive(Clone)]
pub struct DeltaParam {
    pub(crate) delta: u64,
    pub(crate) value: u64,
}
pub type IncrementParam = DeltaParam;
pub type DecrementParam = IncrementParam;

pub type DeltaResultValueType = u64;
#[derive(Debug)]
pub struct DeltaResult {
    pub cas: u32,
    pub value: DeltaResultValueType,
}
/**
 * Implements Memcache commands based
 * on Key Value Store
 */
pub struct MemcStore {
    store: Arc<dyn Cache + Send + Sync>,
}

impl MemcStore {
    pub fn new(store: Arc<dyn Cache + Send + Sync>) -> MemcStore {
        MemcStore { store }
    }

    pub fn set(&self, key: KeyType, record: Record) -> Result<SetStatus> {
        self.store.set(key, record)
    }

    pub fn get(&self, key: &KeyType) -> Result<Record> {
        self.store.get(key)
    }

    // fn touch_record(&self, _record: &mut Record) {
    //     let _timer = self.timer.secs();
    // }

    pub fn add(&self, key: KeyType, record: Record) -> Result<SetStatus> {
        match self.get(&key) {
            Ok(_record) => Err(CacheError::KeyExists),
            Err(_err) => self.set(key, record),
        }
    }

    pub fn replace(&self, key: KeyType, record: Record) -> Result<SetStatus> {
        match self.get(&key) {
            Ok(_record) => self.set(key, record),
            Err(_err) => Err(CacheError::NotFound),
        }
    }

    pub fn append(&self, key: KeyType, new_record: Record) -> Result<SetStatus> {
        match self.get(&key) {
            Ok(mut record) => {
                record.header.cas = new_record.header.cas;
                let mut value =
                    BytesMut::with_capacity(record.value.len() + new_record.value.len());
                value.extend_from_slice(&record.value);
                value.extend_from_slice(&new_record.value);
                record.value = value.freeze();
                self.set(key, record)
            }
            Err(_err) => Err(CacheError::NotFound),
        }
    }

    pub fn prepend(&self, key: KeyType, new_record: Record) -> Result<SetStatus> {
        match self.get(&key) {
            Ok(mut record) => {
                let mut value =
                    BytesMut::with_capacity(record.value.len() + new_record.value.len());
                value.extend_from_slice(&new_record.value);
                value.extend_from_slice(&record.value);
                record.value = value.freeze();
                record.header.cas = new_record.header.cas;
                self.set(key, record)
            }
            Err(_err) => Err(CacheError::NotFound),
        }
    }

    pub fn increment(
        &self,
        header: Meta,
        key: KeyType,
        increment: IncrementParam,
    ) -> Result<DeltaResult> {
        self.add_delta(header, key, increment, true)
    }

    pub fn decrement(
        &self,
        header: Meta,
        key: KeyType,
        decrement: DecrementParam,
    ) -> Result<DeltaResult> {
        self.add_delta(header, key, decrement, false)
    }

    fn add_delta(
        &self,
        header: Meta,
        key: KeyType,
        delta: DeltaParam,
        increment: bool,
    ) -> Result<DeltaResult> {
        match self.get(&key) {
            Ok(mut record) => {
                str::from_utf8(&record.value)
                    .map(|value: &str| {
                        value
                            .parse::<u64>()
                            .map_err(|_err| CacheError::ArithOnNonNumeric)
                    })
                    .map_err(|_err| CacheError::ArithOnNonNumeric)
                    .and_then(|value: std::result::Result<u64, CacheError>| {
                        //flatten result
                        value
                    })
                    .map(|mut value: u64| {
                        if increment {
                            value += delta.delta;
                        } else if delta.delta > value {
                            value = 0;
                        } else {
                            value -= delta.delta;
                        }
                        record.value = Bytes::from(value.to_string());
                        // Don't overwrite the CAS - preserve it for proper CAS checking
                        record.header.timestamp = header.timestamp;
                        record.header.flags = header.flags;
                        record.header.time_to_live = header.time_to_live;
                        self.set(key, record).map(|result| DeltaResult {
                            cas: result.cas,
                            value,
                        })
                    })
                    .and_then(|result: std::result::Result<DeltaResult, CacheError>| {
                        //flatten result
                        result
                    })
            }
            Err(_err) => {
                if header.get_expiration() != 0xffffffff {
                    let record = Record::new(
                        Bytes::from(delta.value.to_string()),
                        0,
                        0,
                        header.get_expiration(),
                    );
                    return self.set(key, record).map(|result| DeltaResult {
                        cas: result.cas,
                        value: delta.value,
                    });
                }
                Err(CacheError::NotFound)
            }
        }
    }

    pub fn delete(&self, key: KeyType, header: Meta) -> Result<Record> {
        self.store.delete(key, header)
    }

    pub fn flush(&self, header: Meta) {
        self.store.flush(header)
    }
}

#[cfg(test)]
mod storage_tests;
