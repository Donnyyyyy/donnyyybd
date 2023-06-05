use std::fs::{File, OpenOptions};

use command_handler::DbServer;
use storage::log_storage::LogStorage;
pub mod command_handler;
pub mod storage;

static FILENAME: &str = "./data/data";

fn main() {
    let fin = File::open(FILENAME).unwrap();
    let fout = OpenOptions::new().append(true).open(FILENAME).unwrap();
    let mut storage = LogStorage::new(&fin, &fout);
    DbServer::start("localhost".to_string(), 1337, &mut storage).unwrap();
}
