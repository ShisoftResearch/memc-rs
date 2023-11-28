use crate::{
    memcache::store::MemcStore, memcache_server::handler::BinaryHandler,
    protocol::binary_codec::BinaryRequest,
};
use std::{
    env,
    fs::{self, File},
    sync::Arc,
    thread,
};

use super::playback_ctl::{Playback, PlaybackReport};
use affinity::{get_core_num, set_thread_affinity};
use minstant::Instant;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

pub fn run_records(ctl: &Arc<Playback>, name: &String, store: &Arc<MemcStore>) -> bool {
    // Asynchrnozed running recording in a seperate thread
    let ctl = ctl.clone();
    let store = store.clone();
    let name = name.clone();
    let dataset = load_record_files(&name);
    if dataset.is_empty() {
        return false;
    }
    thread::spawn(move || {
        let num_threads = dataset.len();
        let all_run_threads = dataset
            .into_iter()
            .enumerate()
            .map(|(tid, (conn_id, data))| {
                let handler = BinaryHandler::new(store.clone());
                thread::Builder::new()
                    .name(format!("Rec-conn-{}", conn_id))
                    .spawn(move || {
                        pin_by_tid(tid, num_threads);
                        let ops = data.len();
                        let mut time_vec = vec![0; ops];
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
                        (
                            tid,
                            conn_id,
                            ops,
                            start_time,
                            start_clock,
                            end_time,
                            end_clock,
                            time_vec,
                        )
                    })
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let all_threads = all_run_threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
            .into_iter()
            .map(
                |(tid, conn_id, ops, start_time, start_clock, end_time, end_clock, time_vec)| {
                    thread::Builder::new()
                        .name(format!("Rec-coil-conn-{}", conn_id))
                        .spawn(move || {
                            pin_by_tid(tid, num_threads);
                            let mut time_coli_vec = vec![0; ops];
                            let coil_start_time = tsc();
                            for i in 0..ops {
                                time_coli_vec[i] = tsc();
                            }
                            let coli_time = tsc() - coil_start_time;
                            let coil_clock_time = Instant::now() - end_clock;
                            let bench_time = end_time - start_time - coli_time;
                            let bench_clock_time = end_clock - start_clock - coil_clock_time;
                            let mut req_time = vec![0; ops];
                            req_time[0] = time_vec[0] - start_time;
                            for i in 1..time_vec.len() {
                                req_time[i] = time_vec[i] - time_vec[i - 1];
                            }
                            let throughput =
                                ops as f64 / bench_clock_time.as_millis() as f64 * 1000f64;
                            (bench_time, bench_clock_time, ops, throughput, req_time)
                        })
                        .unwrap()
                },
            )
            .collect::<Vec<_>>();
        let all_results = all_threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>();
        let all_ops = all_results
            .iter()
            .map(|(_, _, ops, _, _)| *ops)
            .sum::<usize>();
        let all_throughput = all_results
            .iter()
            .map(|(_, _, _, throughput, _)| *throughput)
            .sum::<f64>();
        let all_req_time = all_results
            .iter()
            .map(|(_, _, _, _, req_t)| req_t.clone().into_iter())
            .flatten()
            .collect::<Vec<_>>();
        let (max_time_id, _) = all_results
            .iter()
            .enumerate()
            .max_by_key(|(_, (t, _, _, _, _))| t)
            .unwrap();
        let (max_bench_time_clk, max_bench_time, _, _, _) = all_results[max_time_id];
        let (c90, c99, c99_9, c99_99) = calculate_percentiles(&all_req_time);
        let max_req = *all_req_time.iter().max().unwrap();
        let min_req = *all_req_time.iter().min().unwrap();
        ctl.stop(PlaybackReport {
            ops: all_ops as u64,
            throughput: all_throughput,
            max_time_ms: max_bench_time.as_millis() as u64,
            max_time_clk: max_bench_time_clk,
            c90,
            c99,
            c99_9,
            c99_99,
            max: max_req,
            min: min_req,
        });
    });
    return true;
}

fn load_record_files(name: &String) -> Vec<(u64, Vec<BinaryRequest>)> {
    let working_dir = env::current_dir().unwrap();
    let full_file = working_dir.join(name);
    let full_path = full_file.as_path();
    let path_dir = full_path.parent().unwrap();
    let shorten_name = full_path.file_name().unwrap().to_str().unwrap();
    let dir_files = fs::read_dir(path_dir).unwrap();
    let res = dir_files
        .filter_map(|dir_entry| {
            let file_path = dir_entry.unwrap();
            let filename = {
                let filename_buff = file_path
                    .path()
                    .strip_prefix(path_dir)
                    .unwrap()
                    .to_path_buf();
                filename_buff.to_str().unwrap().to_string()
            };
            if filename.starts_with(&format!("{}-", shorten_name)) && filename.ends_with(".bin") {
                return Some((filename, file_path));
            } else {
                return None;
            }
        })
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(|(filename, file_path)| {
            let name_comps = filename.split("-").collect::<Vec<_>>();
            assert_eq!(name_comps.len(), 3);
            let conn_id: u64 = name_comps[1]
                .parse()
                .unwrap_or_else(|_| panic!("{:?}", name_comps));
            let file = File::open(file_path.path()).unwrap();
            let data: Vec<BinaryRequest> = bincode::deserialize_from(file).unwrap();
            (conn_id, data)
        })
        .collect::<Vec<_>>();
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

fn pin_by_tid(tid: usize, num_t: usize) {
    let num_cores = get_core_num();
    let core_assign_step = num_cores / num_t;
    set_thread_affinity(&vec![tid * core_assign_step]).unwrap();
}
