use super::cli::parser::Engine;
use super::eviction_policy::EvictionPolicy;
use super::random_policy::RandomPolicy;
use crate::cache::cache::Cache;
use crate::memory_store::backends::cht::ChtMapBackend;
use crate::memory_store::backends::contrie::ContrieBackend;
use crate::memory_store::backends::cuckoo::CuckooBackend;
use crate::memory_store::backends::dashmap::DashMapBackend;
use crate::memory_store::backends::flurry::FlurryMapBackend;
use crate::memory_store::backends::lightning::LightningBackend;
use crate::memory_store::backends::scc::SccHashMapBackend;
use crate::memory_store::store::MemoryStore;
use crate::server::timer;
use std::cmp::max;
use std::sync::Arc;

pub struct MemcacheStoreConfig {
    policy: EvictionPolicy,
    memory_limit: u64,
    capacity: usize,
    engine: Engine,
}

impl MemcacheStoreConfig {
    pub fn new(memory_limit: u64, capacity: usize, engine: Engine) -> MemcacheStoreConfig {
        MemcacheStoreConfig {
            policy: EvictionPolicy::None,
            memory_limit,
            capacity,
            engine,
        }
    }
}

#[derive(Default)]
pub struct MemcacheStoreBuilder {}

impl MemcacheStoreBuilder {
    pub fn new() -> MemcacheStoreBuilder {
        MemcacheStoreBuilder {}
    }

    pub fn from_config(
        config: MemcacheStoreConfig,
        timer: Arc<dyn timer::Timer + Send + Sync>,
    ) -> Arc<dyn Cache + Send + Sync> {
        let store_engine = Self::backend_from_config(&config, timer);
        let store: Arc<dyn Cache + Send + Sync> = match config.policy {
            EvictionPolicy::Random => {
                Arc::new(RandomPolicy::new(store_engine, config.memory_limit))
            }
            EvictionPolicy::None => store_engine,
        };
        store
    }

    fn backend_from_config(
        config: &MemcacheStoreConfig,
        timer: Arc<dyn timer::Timer + Send + Sync>,
    ) -> Arc<dyn Cache + Send + Sync> {
        let esti_cap = config.capacity;
        let cap = max(esti_cap, 8192) as usize;
        match config.engine {
            Engine::Lightning => Arc::new(MemoryStore::<LightningBackend>::new(timer, cap)),
            Engine::DashMap => Arc::new(MemoryStore::<DashMapBackend>::new(timer, cap)),
            Engine::Cuckoo => Arc::new(MemoryStore::<CuckooBackend>::new(timer, cap)),
            Engine::Concach => unimplemented!(),
            Engine::Cht => Arc::new(MemoryStore::<ChtMapBackend>::new(timer, cap)),
            Engine::SccHashMap => Arc::new(MemoryStore::<SccHashMapBackend>::new(timer, cap)),
            Engine::Contrie => Arc::new(MemoryStore::<ContrieBackend>::new(timer, cap)),
            Engine::Flurry => Arc::new(MemoryStore::<FlurryMapBackend>::new(timer, cap)),
        }
    }
}
