use log::info;
use std::env;
use std::process;
use std::sync::Arc;
extern crate clap;
extern crate memcrs;

#[cfg(feature = "jemallocator")]
use jemallocator::Jemalloc;

#[cfg(feature = "jemallocator")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(feature = "bumpallocator")]
#[global_allocator]
static GLOBAL: bump_allocator::BumpPointer = bump_allocator::BumpPointer;

fn get_log_level_filter(verbose: u8) -> log::LevelFilter {
    // Vary the output based on how many times the user used the "verbose" flag
    // (i.e. 'myprog -v -v -v' or 'myprog -vvv' vs 'myprog -v'
    match verbose {
        0 => log::LevelFilter::Error,
        1 => log::LevelFilter::Warn,
        2 => log::LevelFilter::Info,
        3 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    }
}

fn main() {
    let cli_config = match memcrs::memcache::cli::parser::parse(env::args().collect()) {
        Ok(config) => config,
        Err(err) => {
            eprint!("{}", err);
            process::exit(1);
        }
    };

    // Initialize env_logger with the log level based on verbosity
    env_logger::Builder::from_default_env()
        .filter_level(get_log_level_filter(cli_config.verbose))
        .init();

    info!("Listen address: {}", cli_config.listen_address.to_string());
    info!("Listen port: {}", cli_config.port);
    info!("Number of threads: {}", cli_config.threads);
    info!("Runtime type: {}", cli_config.runtime_type.as_str());
    info!(
        "Max item size: {}",
        cli_config
            .item_size_limit
            .get_appropriate_unit(true)
            .to_string()
    );
    info!(
        "Memory limit: {}",
        byte_unit::Byte::from_bytes(cli_config.memory_limit.into())
            .get_appropriate_unit(true)
            .to_string()
    );

    let system_timer: Arc<memcrs::server::timer::SystemTimer> =
        Arc::new(memcrs::server::timer::SystemTimer::new());
    let parent_runtime = memcrs::memcache_server::runtime_builder::create_memcrs_server(
        cli_config,
        system_timer.clone(),
    );
    parent_runtime.block_on(system_timer.run())
}
