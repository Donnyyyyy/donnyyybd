pub mod log_storage;

use std::error;


pub trait Storage {
    fn get(&mut self, key: String) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>>;
    fn set(&mut self, key: String, value: &[u8], size: usize) -> Result<(), Box<dyn error::Error>>;
}
