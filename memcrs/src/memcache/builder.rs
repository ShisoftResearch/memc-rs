use super::cli::parser::Engine;
use super::eviction_policy::EvictionPolicy;
use super::random_policy::RandomPolicy;
use crate::cache::cache::Cache;
use crate::memory_store::backends::lightning::LightningBackend;
use crate::memory_store::store::{DefaultMemoryStore, MemoryStore};
use crate::server::timer;
use std::cmp::max;
use std::sync::Arc;

pub struct MemcacheStoreConfig {
    policy: EvictionPolicy,
    memory_limit: u64,
    engine: Engine,
}

impl MemcacheStoreConfig {
    pub fn new(memory_limit: u64, engine: Engine) -> MemcacheStoreConfig {
        MemcacheStoreConfig {
            policy: EvictionPolicy::None,
            memory_limit,
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
        let cap = max(config.memory_limit * 1024 * 1024 / 1024, 8192) as usize;
        match config.engine {
            Engine::Lightning => Arc::new(MemoryStore::<LightningBackend>::new(timer, cap)),
            Engine::DashMap => todo!(),
            Engine::Cuckoo => todo!(),
            Engine::Concach => todo!(),
            Engine::Cht => todo!(),
            Engine::SccHashMap => todo!(),
            Engine::Contrie => todo!(),
            Engine::Flurry => todo!(),
        }
    }
}
