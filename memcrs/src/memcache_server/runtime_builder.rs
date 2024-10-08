extern crate core_affinity;
use crate::control_plane;
use crate::memcache;
use crate::memcache::cli::parser::RuntimeType;
use crate::memcache::store::MemcStore;
use crate::memcache_server;
use crate::server;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::runtime::Builder;

use crate::memcache::cli::parser::MemcrsArgs;

use super::recorder::MasterRecorder;

fn get_worker_thread_name() -> String {
    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
    let str = format!("memcrsd-wrk-{}", id);
    str
}

fn create_multi_thread_runtime(worker_threads: usize) -> tokio::runtime::Runtime {
    let runtime = Builder::new_multi_thread()
        .thread_name_fn(get_worker_thread_name)
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .unwrap();
    runtime
}

fn create_current_thread_runtime() -> tokio::runtime::Runtime {
    let runtime = Builder::new_current_thread()
        //.worker_threads(threads as usize)
        .thread_name_fn(get_worker_thread_name)
        //.max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime
}

fn create_current_thread_server(
    config: MemcrsArgs,
    store: Arc<MemcStore>,
    recorder: &Arc<MasterRecorder>,
) -> tokio::runtime::Runtime {
    let addr = SocketAddr::new(config.listen_address, config.port);
    let memc_config = memcache_server::memc_tcp::MemcacheServerConfig::new(
        60,
        config.item_size_limit.get_bytes() as u32,
        config.backlog_limit,
    );

    let core_ids = core_affinity::get_core_ids().unwrap();
    for i in 0..config.threads {
        let store_rc = store.clone();
        let core_ids_clone = core_ids.clone();
        let recorder = recorder.clone();
        std::thread::spawn(move || {
            debug!("Creating runtime {}", i);
            let core_id = core_ids_clone[i % core_ids_clone.len()];
            let res = core_affinity::set_for_current(core_id);
            let create_runtime = || {
                let child_runtime = create_current_thread_runtime();
                let mut tcp_server = memcache_server::memc_tcp::MemcacheTcpServer::new(
                    memc_config,
                    store_rc,
                    &recorder,
                );
                child_runtime.block_on(tcp_server.run(addr)).unwrap()
            };
            if res {
                debug!(
                    "Thread pinned {:?} to core {:?}",
                    std::thread::current().id(),
                    core_id.id
                );
                create_runtime();
            } else {
                warn!("Cannot pin thread to core {}", core_id.id);
                create_runtime();
            }
        });
    }
    create_current_thread_runtime()
}

fn create_threadpool_server(
    config: MemcrsArgs,
    store: Arc<MemcStore>,
    recorder: &Arc<MasterRecorder>,
) -> tokio::runtime::Runtime {
    let addr = SocketAddr::new(config.listen_address, config.port);
    let memc_config = memcache_server::memc_tcp::MemcacheServerConfig::new(
        60,
        config.item_size_limit.get_bytes() as u32,
        config.backlog_limit,
    );
    let runtime = create_multi_thread_runtime(config.threads);
    let mut tcp_server =
        memcache_server::memc_tcp::MemcacheTcpServer::new(memc_config, store, recorder);
    runtime.spawn(async move { tcp_server.run(addr).await });
    runtime
}

pub fn create_memcrs_server(
    config: MemcrsArgs,
    system_timer: std::sync::Arc<server::timer::SystemTimer>,
) -> tokio::runtime::Runtime {
    let store_config = memcache::builder::MemcacheStoreConfig::new(
        config.memory_limit,
        config.capacity,
        config.engine,
    );
    let memcache_store =
        memcache::builder::MemcacheStoreBuilder::from_config(store_config, system_timer);
    let recorder = Arc::new(MasterRecorder::new());
    let storeage = Arc::new(MemcStore::new(memcache_store));
    control_plane::start_service(&recorder, &storeage);
    match config.runtime_type {
        RuntimeType::CurrentThread => create_current_thread_server(config, storeage, &recorder),
        RuntimeType::MultiThread => create_threadpool_server(config, storeage, &recorder),
    }
}
