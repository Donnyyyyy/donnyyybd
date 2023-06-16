use server::DbServer;

pub mod server;
pub mod utils;
pub mod storage;

fn main() {
    env_logger::init();

    DbServer::new("localhost".to_string(), 1337)
        .unwrap()
        .start()
        .unwrap();
}
