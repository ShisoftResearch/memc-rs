use super::cli::parser::Backend;
use super::eviction_policy::EvictionPolicy;
use super::random_policy::RandomPolicy;
use crate::cache::cache::Cache;
use crate::memory_store::backends::lightning::LightningBackend;
use crate::memory_store::store::{MemoryStore, DefaultMemoryStore};
use crate::server::timer;
use std::cmp::max;
use std::sync::Arc;

pub struct MemcacheStoreConfig {
    policy: EvictionPolicy,
    memory_limit: u64,
    backend: Backend
}

impl MemcacheStoreConfig {
    pub fn new(memory_limit: u64, backend: Backend) -> MemcacheStoreConfig {
        MemcacheStoreConfig {
            policy: EvictionPolicy::None,
            memory_limit,
            backend
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

    fn backend_from_config(config: &MemcacheStoreConfig, timer: Arc<dyn timer::Timer + Send + Sync>) -> Arc<dyn Cache + Send + Sync> {
        let cap = max(config.memory_limit * 1024 * 1024 / 1024, 8192) as usize;
        match config.backend {
            Backend::Lightning => Arc::new(MemoryStore::<LightningBackend>::new(timer, cap)),
            Backend::DashMap => todo!(),
            Backend::Cuckoo => todo!(),
            Backend::Concach => todo!(),
            Backend::Cht => todo!(),
            Backend::SccHashMap => todo!(),
            Backend::Contrie => todo!(),
            Backend::Flurry => todo!(),
        }
    }
}
