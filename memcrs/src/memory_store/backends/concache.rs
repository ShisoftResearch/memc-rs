use bytes::Bytes;
use crate::{memcache::store::*, cache::error::CacheError, memory_store::store::Peripherals};
use super::StorageBackend;

pub struct ConcacheBackend(concache::manual::MapHandle<KeyType, Record>);

// Concache only support key implement Copy