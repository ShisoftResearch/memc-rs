use crate::{
    memcache::store::MemcStore, memcache_server::handler::BinaryHandler,
    protocol::binary_codec::BinaryRequest,
};
use std::{
    env,
    fs::{self, File},
    io,
    sync::Arc,
    thread, time::Duration,
};

use super::playback_ctl::{Playback, PlaybackReport};
use affinity::{get_core_num, set_thread_affinity};
use flate2::read::ZlibDecoder;
use minstant::Instant;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::ThreadPoolBuilder;

pub fn run_records(ctl: &Arc<Playback>, name: &String, store: &Arc<MemcStore>, iters: u32) -> bool {
    // Asynchrnozed running recording in a seperate thread
    let ctl = ctl.clone();
    let store = store.clone();
    let name = name.clone();
    let dataset = match load_record_files(&name) {
        Ok(ds) => ds,
        Err(e) => {
            eprintln!("Failed to load record files: {}", e);
            return false;
        }
    };
    if dataset.is_empty() {
        return false;
    }
    thread::spawn(move || {
        let dataset_ref = dataset
            .iter()
            .map(|(ref id, ref reqs)| {
                let new_reqs = reqs.iter().cloned().collect::<Vec<_>>();
                (*id, new_reqs)
            }) // Shallow clone here to avoid bring memory allocation to the backend
            .collect::<Vec<_>>()
            .into_iter();
        let num_threads = dataset.len();
        let all_run_threads = dataset_ref
            .enumerate()
            .map(|(tid, (conn_id, mut data))| {
                let handler = BinaryHandler::new(store.clone());
                thread::Builder::new()
                    .name(format!("Rec-conn-{}", conn_id))
                    .spawn(move || {
                        pin_by_tid(tid, num_threads);
                        let data_len = data.len();
                        (1..iters).for_each(|_| {
                            // Replicate dataset
                            data.append(&mut data[0..data_len].iter().cloned().collect());
                        });
                        let ops = data.len();
                        let mut time_vec = vec![Duration::from_nanos(0); ops];
                        let mut idx = 0;
                        for req in data {
                            let (_, duration) = handler.handle_request(req);
                            if let Some(duration) = duration {
                                time_vec[idx] = duration;
                            }
                            idx = idx + 1;
                        }
                        (
                            tid,
                            conn_id,
                            ops,
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
                |(tid, conn_id, ops, time_vec)| {
                    thread::Builder::new()
                        .name(format!("Rec-coil-conn-{}", conn_id))
                        .spawn(move || {
                            pin_by_tid(tid, num_threads);
                            let bench_clock_time = time_vec.iter().sum::<Duration>();
                            let throughput =
                                ops as f64 / bench_clock_time.as_nanos() as f64 * 1e+9f64;
                            (bench_clock_time, ops, throughput, time_vec)
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
            .map(|(_, ops, _, _)| *ops)
            .sum::<usize>();
        let all_throughput = all_results
            .iter()
            .map(|(_, _, throughput, _)| *throughput)
            .sum::<f64>();
        let all_req_time = all_results
            .iter()
            .map(|(_, _, _, req_t)| req_t.clone().into_iter())
            .flatten()
            .collect::<Vec<_>>();
        let (c50, c90, c99, c99_9, c99_99) = calculate_percentiles(&all_req_time);
        let max_req = *all_req_time.iter().max().unwrap();
        let min_req = *all_req_time.iter().min().unwrap();
        let avg = all_req_time.iter().sum::<Duration>().as_nanos() as f64 / all_req_time.len() as f64;
        ctl.stop(PlaybackReport {
            ops: all_ops as u64,
            throughput: all_throughput,
            avg,
            c50,
            c90,
            c99,
            c99_9,
            c99_99,
            max: max_req.as_nanos() as u64,
            min: min_req.as_nanos() as u64,
        });
        ctl.req_history.lock().push(dataset); // finally, record the dataset so memory allocation would be minimal
    });
    return true;
}

fn load_record_files(name: &String) -> io::Result<Vec<(u64, Vec<BinaryRequest>)>> {
    let working_dir = env::current_dir()?;
    let full_file = working_dir.join(name);
    let full_path = full_file.as_path();
    let path_dir = full_path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "input path has no parent"))?;
    let shorten_name = full_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid filename"))?;
    let dir_files = fs::read_dir(path_dir)?;

    // Collect only filename strings and path buffers (avoid carrying DirEntry/Fds)
    let candidates = dir_files
        .filter_map(|dir_entry_res| {
            let dir_entry = match dir_entry_res {
                Ok(d) => d,
                Err(_) => return None,
            };
            let path = dir_entry.path();
            let filename = match path
                .strip_prefix(path_dir)
                .ok()
                .and_then(|p| p.to_str())
            {
                Some(s) => s.to_string(),
                None => return None,
            };
            if filename.starts_with(&format!("{}-", shorten_name)) && filename.ends_with(".bin") {
                Some((filename, path))
            } else {
                None
            }
        })
        .collect::<Vec<(String, std::path::PathBuf)>>();

    // Limit concurrency: at most 16 parallel file loads
    let pool = ThreadPoolBuilder::new()
        .num_threads(16)
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let results = pool.install(|| {
        candidates
            .into_par_iter()
            .map(|(filename, path)| -> io::Result<(u64, Vec<BinaryRequest>)> {
                let name_comps = filename.split('-').collect::<Vec<_>>();
                if name_comps.len() != 3 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("bad filename: {}", filename),
                    ));
                }
                let conn_id: u64 = name_comps[1]
                    .parse()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                // Keep file lifetime as short as possible
                let data: Vec<BinaryRequest> = {
                    let file = File::open(&path)?;
                    let compress_decoder = ZlibDecoder::new(file);
                    bincode::deserialize_from(compress_decoder)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                };
                Ok((conn_id, data))
            })
            .collect::<Vec<io::Result<(u64, Vec<BinaryRequest>)>>>()
    });

    // Propagate the first error (if any) to the caller, otherwise return the dataset
    let mut out = Vec::with_capacity(results.len());
    for r in results {
        out.push(r?);
    }
    Ok(out)
}

#[inline]
fn tsc() -> u64 {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::_rdtsc;
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::_rdtsc;

    unsafe { _rdtsc() }
}

fn calculate_percentiles(latencies: &Vec<Duration>) -> (u64, u64, u64, u64, u64) {
    // Sort the latencies
    let mut latencies = latencies.clone();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // Helper function to calculate a percentile
    fn percentile(latencies: &[Duration], p: f64) -> u64 {
        let len = latencies.len() as f64;
        let index = (p as f64 / 100.0 * len).ceil() as usize - 1; // Adjust for zero-based index
        latencies[index.min(latencies.len() - 1)].as_nanos() as u64 // Handle edge case
    }

    // Calculate percentiles
    let c50 = percentile(&latencies, 50.0);
    let c90 = percentile(&latencies, 90.0);
    let c99 = percentile(&latencies, 99.0);
    let c99_9 = percentile(&latencies, 99.9);
    let c99_99 = percentile(&latencies, 99.99);

    (c50, c90, c99, c99_9, c99_99)
}

fn pin_by_tid(tid: usize, num_t: usize) {
    let num_cores = get_core_num();
    let core_assign_step = num_cores / num_t;
    set_thread_affinity(&vec![tid * core_assign_step]).unwrap();
}
