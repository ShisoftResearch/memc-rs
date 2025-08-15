use super::StorageBackend;
use crate::{cache::error::CacheError, memcache::store::*, memory_store::store::Peripherals};


pub struct ConcacheBackend(concache::manual::MapHandle<KeyType, Record>);

// Concache only support key implement Copy
