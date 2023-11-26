use crate::cache::cache::{CacheMetaData, CachePredicate, SetStatus};
use crate::cache::error::Result;
use crate::memcache::store::{KeyType, Record};

use super::store::Peripherals;

pub mod dashmap;
pub mod lightning;

pub trait StorageBackend {
    fn init(cap: usize) -> Self;
    fn get(&self, key: &KeyType) -> Result<Record>;
    fn remove(&self, key: &KeyType) -> Option<Record>;
    fn set(&self, key: KeyType, record: Record, peripherals: &Peripherals) -> Result<SetStatus>;
    fn delete(&self, key: KeyType, header: CacheMetaData) -> Result<Record>;
    fn flush(&self, header: CacheMetaData);
    fn len(&self) -> usize;
    fn predict_keys(&self, f: &mut CachePredicate) -> Vec<KeyType>;
}
