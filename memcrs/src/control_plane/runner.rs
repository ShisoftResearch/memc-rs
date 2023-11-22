use crate::{protocol::binary_codec::BinaryRequest, memcache::store::{self, MemcStore}, memcache_server::handler::BinaryHandler};
use std::{
    env,
    fs::{self, File},
    thread, sync::Arc,
};
use tracing_subscriber::fmt::format;

use super::playback_ctl::Playback;

pub fn run_records(ctl: &Arc<Playback>, name: &String, benchmarking: bool, store: &Arc<MemcStore>) {
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
                        for req in data {
                            handler.handle_request(req);
                        }
                    })
            })
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
