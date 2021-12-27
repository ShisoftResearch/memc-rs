use super::error::{StorageError, StorageResult};
use super::timer;
use bytes::Bytes;
use dashmap::mapref::multiple::RefMulti;
use dashmap::{DashMap, ReadOnlyView};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Meta {
    pub(self) timestamp: u64,
    pub(crate) cas: u64,
    pub(crate) flags: u32,
    pub(self) time_to_live: u32,
}

impl Meta {
    pub fn new(cas: u64, flags: u32, time_to_live: u32) -> Meta {
        Meta {
            timestamp: 0,
            cas,
            flags,
            time_to_live,
        }
    }

    pub fn get_expiration(&self) -> u32 {
        self.time_to_live
    }

    pub const fn len(&self) -> usize {
        std::mem::size_of::<Meta>()
    }

    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub type ValueType = Bytes;

#[derive(Clone, Debug)]
pub struct Record {
    pub(crate) header: Meta,
    pub(crate) value: ValueType,
}

impl Record {
    pub fn new(value: ValueType, cas: u64, flags: u32, expiration: u32) -> Record {
        let header = Meta::new(cas, flags, expiration);
        Record { header, value }
    }

    pub fn len(&self) -> usize {
        self.header.len() + self.value.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

#[derive(Debug)]
pub struct SetStatus {
    pub cas: u64,
}

pub type KeyType = Bytes;

// Read only view over a store
pub trait KVStoreReadOnlyView<'a> {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn keys(&'a self) -> Box<dyn Iterator<Item = &'a KeyType> + 'a>;
}

// Not a part of Store public API
pub mod impl_details {
    use super::*;
    pub trait StoreImplDetails {
        //
        fn get_by_key(&self, key: &KeyType) -> StorageResult<Record>;

        //
        fn check_if_expired(&self, key: &KeyType, record: &Record) -> bool;
    }
}

pub type RemoveIfResult = Vec<Option<(KeyType, Record)>>;
pub type Predicate = dyn FnMut(&KeyType, &Record) -> bool;
// An abstraction over a generic store key <=> value store
pub trait KVStore: impl_details::StoreImplDetails {
    // Returns a value associated with a key
    fn get(&self, key: &KeyType) -> StorageResult<Record> {
        let result = self.get_by_key(key);
        match result {
            Ok(record) => {
                if self.check_if_expired(key, &record) {
                    return Err(StorageError::NotFound);
                }
                Ok(record)
            }
            Err(err) => Err(err),
        }
    }

    // Sets value that will be associated with a store.
    // If value already exists in a store CAS field is compared
    // and depending on CAS value comparison value is set or rejected.
    //
    // - if CAS is equal to 0 value is always set
    // - if CAS is not equal value is not set and there is an error
    //   returned with status KeyExists
    fn set(&self, key: KeyType, record: Record) -> StorageResult<SetStatus>;

    // Removes a value associated with a key a returns it to a caller if CAS
    // value comparison is successful or header.CAS is equal to 0:
    //
    // - if header.CAS != to stored record CAS KeyExists is returned
    // - if key is not found NotFound is returned
    fn delete(&self, key: KeyType, header: Meta) -> StorageResult<Record>;

    // Removes all values from a store
    //
    // - if header.ttl is set to 0 values are removed immediately,
    // - if header.ttl>0 values are removed from a store after
    //   ttl expiration
    fn flush(&self, header: Meta);

    // Number of key value pairs stored in store
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    // Returns a read-only view over a stroe
    fn as_read_only(&self) -> Box<dyn KVStoreReadOnlyView>;

    // Removes key-value pairs from a store for which
    // f predicate returns true
    fn remove_if(&self, f: &mut Predicate) -> RemoveIfResult;

    // Removes key value and returns as an option
    fn remove(&self, key: &KeyType) -> Option<(KeyType, Record)>;
}

type Storage = DashMap<KeyType, Record>;
pub struct KeyValueStore {
    memory: Storage,
    timer: Arc<dyn timer::Timer + Send + Sync>,
    cas_id: AtomicU64,
}

type StorageReadOnlyView = ReadOnlyView<KeyType, Record>;

impl<'a> KVStoreReadOnlyView<'a> for StorageReadOnlyView {
    fn len(&self) -> usize {
        StorageReadOnlyView::len(self)
    }

    fn is_empty(&self) -> bool {
        StorageReadOnlyView::is_empty(self)
    }

    fn keys(&'a self) -> Box<dyn Iterator<Item = &'a KeyType> + 'a> {
        let keys = self.keys();
        Box::new(keys)
    }
}

impl KeyValueStore {
    pub fn new(timer: Arc<dyn timer::Timer + Send + Sync>) -> KeyValueStore {
        KeyValueStore {
            memory: DashMap::new(),
            timer,
            cas_id: AtomicU64::new(1),
        }
    }

    fn get_cas_id(&self) -> u64 {
        self.cas_id.fetch_add(1, Ordering::SeqCst) as u64
    }
}

impl impl_details::StoreImplDetails for KeyValueStore {
    fn get_by_key(&self, key: &KeyType) -> StorageResult<Record> {
        match self.memory.get(key) {
            Some(record) => Ok(record.clone()),
            None => Err(StorageError::NotFound),
        }
    }

    fn check_if_expired(&self, key: &KeyType, record: &Record) -> bool {
        let current_time = self.timer.timestamp();

        if record.header.time_to_live == 0 {
            return false;
        }

        if record.header.timestamp + (record.header.time_to_live as u64) > current_time {
            return false;
        }
        match self.remove(key) {
            Some(_) => true,
            None => true,
        }
    }
}

impl KVStore for KeyValueStore {
    // Removes key value and returns as an option
    fn remove(&self, key: &KeyType) -> Option<(KeyType, Record)> {
        self.memory.remove(key)
    }

    fn set(&self, key: KeyType, mut record: Record) -> StorageResult<SetStatus> {
        //trace!("Set: {:?}", &record.header);
        if record.header.cas > 0 {
            match self.memory.get_mut(&key) {
                Some(mut key_value) => {
                    if key_value.header.cas != record.header.cas {
                        Err(StorageError::KeyExists)
                    } else {
                        record.header.cas += 1;
                        record.header.timestamp = self.timer.timestamp();
                        let cas = record.header.cas;
                        *key_value = record;
                        Ok(SetStatus { cas })
                    }
                }
                None => {
                    record.header.cas += 1;
                    record.header.timestamp = self.timer.timestamp();
                    let cas = record.header.cas;
                    self.memory.insert(key, record);
                    Ok(SetStatus { cas })
                }
            }
        } else {
            let cas = self.get_cas_id();
            record.header.cas = cas;
            record.header.timestamp = self.timer.timestamp();
            self.memory.insert(key, record);
            Ok(SetStatus { cas })
        }
    }

    fn delete(&self, key: KeyType, header: Meta) -> StorageResult<Record> {
        let mut cas_match: Option<bool> = None;
        match self.memory.remove_if(&key, |_key, record| -> bool {
            let result = header.cas == 0 || record.header.cas == header.cas;
            cas_match = Some(result);
            result
        }) {
            Some(key_value) => Ok(key_value.1),
            None => match cas_match {
                Some(_value) => Err(StorageError::KeyExists),
                None => Err(StorageError::NotFound),
            },
        }
    }

    fn flush(&self, header: Meta) {
        if header.time_to_live > 0 {
            self.memory.alter_all(|_key, mut value| {
                value.header.time_to_live = header.time_to_live;
                value
            });
        } else {
            self.memory.clear();
        }
    }

    fn as_read_only(&self) -> Box<dyn KVStoreReadOnlyView> {
        let storage_clone = self.memory.clone();
        Box::new(storage_clone.into_read_only())
    }

    fn remove_if(&self, f: &mut Predicate) -> RemoveIfResult {
        let items: Vec<KeyType> = self
            .memory
            .iter()
            .filter(|record: &RefMulti<KeyType, Record>| f(record.key(), record.value()))
            .map(|record: RefMulti<KeyType, Record>| record.key().clone())
            .collect();

        let result: Vec<Option<(KeyType, Record)>> =
            items.iter().map(|key: &KeyType| self.remove(key)).collect();
        result
    }

    fn len(&self) -> usize {
        self.memory.len()
    }

    fn is_empty(&self) -> bool {
        self.memory.is_empty()
    }
}
