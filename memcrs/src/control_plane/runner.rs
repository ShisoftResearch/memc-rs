use crate::{
    memcache::store::{self, MemcStore},
    memcache_server::handler::BinaryHandler,
    protocol::binary_codec::BinaryRequest,
};
use std::{
    env,
    fs::{self, File},
    sync::Arc,
    thread,
};

use super::playback_ctl::Playback;
use minstant::Instant;

pub fn run_records(ctl: &Arc<Playback>, name: &String, store: &Arc<MemcStore>) {
    // Asynchrnozed running recording in a seperate thread
    let ctl = ctl.clone();
    let store = store.clone();
    let name = name.clone();
    ctl.start(&name);
    thread::spawn(move || {
        let dataset = load_record_files(&name);
        let all_threads = dataset
            .into_iter()
            .map(|(conn_id, data)| {
                let handler = BinaryHandler::new(store.clone());
                thread::Builder::new()
                    .name(format!("Rec-conn-{}", conn_id))
                    .spawn(move || {
                        let ops = data.len();
                        let mut time_vec = vec![0; ops];
                        let mut time_coli_vec = vec![0; ops];
                        let mut idx = 0;
                        let start_clock = Instant::now();
                        let start_time = tsc();
                        for req in data {
                            handler.handle_request(req);
                            time_vec[idx] = tsc();
                            idx = idx + 1;
                        }
                        let end_time = tsc();
                        let end_clock = Instant::now();
                        let coil_start_time = tsc();
                        for i in 0..ops {
                            time_coli_vec[i] = tsc();
                        }
                        let coli_time = tsc() - coil_start_time;
                        let coil_clock_time = Instant::now() - end_clock;
                        let bench_time = end_time - start_time - coli_time;
                        let bench_clock_time = end_clock - start_clock - coil_clock_time;
                        let mut req_time = vec![time_vec[0] - start_time];
                        for i in 1..time_vec.len() {
                            req_time[i] = time_vec[i] - time_vec[i - 1];
                        }
                        let latency_precentiles = calculate_percentiles(&req_time);
                        (start_time, end_time, bench_time, bench_clock_time, ops, req_time, latency_precentiles)
                    })
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let all_results = all_threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>();
        ctl.stop();
    });
}

fn load_record_files(name: &String) -> Vec<(u64, Vec<BinaryRequest>)> {
    let mut res = vec![];
    let working_dir = env::current_dir().unwrap();
    let full_file = working_dir.join(name);
    let full_path = full_file.as_path();
    let path_dir = full_path.parent().unwrap();
    let shorten_name = full_path.file_name().unwrap().to_str().unwrap();
    let dir_files = fs::read_dir(path_dir).unwrap();
    for dir_entry in dir_files {
        let file_path = dir_entry.unwrap();
        let filename_buff = file_path
            .path()
            .strip_prefix(path_dir)
            .unwrap()
            .to_path_buf();
        let filename = filename_buff.to_str().unwrap();
        if filename.starts_with(&format!("{}-", shorten_name)) && filename.ends_with(".bin") {
            let name_comps = filename.split("-").collect::<Vec<_>>();
            assert_eq!(name_comps.len(), 2);
            let conn_id: u64 = name_comps[1].parse().unwrap();
            let file = File::open(file_path.path()).unwrap();
            let data: Vec<BinaryRequest> = bincode::deserialize_from(file).unwrap();
            res.push((conn_id, data));
        }
    }
    return res;
}

#[inline]
fn tsc() -> u64 {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::_rdtsc;
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::_rdtsc;

    unsafe { _rdtsc() }
}

fn calculate_percentiles(latencies: &Vec<u64>) -> (u64, u64, u64, u64) {
    // Sort the latencies
    let mut latencies = latencies.clone();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // Helper function to calculate a percentile
    fn percentile(latencies: &[u64], p: f64) -> u64 {
        let len = latencies.len() as f64;
        let index = (p as f64 / 100.0 * len).ceil() as usize - 1; // Adjust for zero-based index
        latencies[index.min(latencies.len() - 1)] // Handle edge case
    }

    // Calculate percentiles
    let c90 = percentile(&latencies, 90.0);
    let c99 = percentile(&latencies, 99.0);
    let c99_9 = percentile(&latencies, 99.9);
    let c99_99 = percentile(&latencies, 99.99);

    (c90, c99, c99_9, c99_99)
}