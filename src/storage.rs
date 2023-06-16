use std::{
    error::Error,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    mem::transmute,
    sync::{Arc, Mutex},
};

use crate::utils::compare_keys;

const KEY_SIZE: usize = 128;
const FILENAME: &str = "./data/data";

pub struct StorageClient {
    reader: BufReader<File>,
    writer_mtx: Arc<Mutex<BufWriter<File>>>,
}

impl StorageClient {
    pub fn new_writer() -> Result<BufWriter<File>, Box<dyn Error>> {
        let file = match OpenOptions::new().append(true).open(FILENAME) {
            Ok(val) => val,
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => File::create(FILENAME),
                _ => return Err(Box::new(err)),
            }?,
        };
        Ok(BufWriter::new(file))
    }

    pub fn new_reader() -> Result<BufReader<File>, Box<dyn Error>> {
        let file: File = File::open(FILENAME)?;
        Ok(BufReader::new(file))
    }

    pub fn new(writer_mtx: Arc<Mutex<BufWriter<File>>>) -> Result<StorageClient, Box<dyn Error>> {
        Ok(StorageClient {
            reader: StorageClient::new_reader()?,
            writer_mtx: writer_mtx,
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let target_key_bytes = key.as_bytes();

        self.reader.seek(SeekFrom::Start(0))?;

        let mut key_buf = [0; KEY_SIZE];
        let mut size_buf: [u8; 8] = [0; 8];
        let mut result: Option<Vec<u8>> = None;

        loop {
            match self.reader.read_exact(&mut key_buf) {
                Ok(_) => {}
                Err(error) => match error.kind() {
                    ErrorKind::UnexpectedEof => {
                        return Ok(result);
                    }
                    _ => {
                        return Err(Box::new(error));
                    }
                },
            };
            self.reader.read_exact(&mut size_buf)?;
            let value_size = usize::from_le_bytes(size_buf);

            if compare_keys(target_key_bytes, &key_buf, KEY_SIZE) {
                let mut value = vec![0u8; value_size];
                self.reader.read_exact(&mut value)?;
                result = Some(value);
            } else {
                self.reader.seek_relative(value_size.try_into()?)?;
            }
        }
    }

    pub fn set(
        &mut self,
        key: String,
        value: &[u8],
        size: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key_bytes = key.as_bytes();
        {
            let mut writer = self.writer_mtx.lock().unwrap();

            writer.write_all(key_bytes)?;

            for _ in 0..KEY_SIZE - key_bytes.len() {
                writer.write(&[0])?;
            }

            let size_bytes: [u8; 8] = unsafe { transmute(size.to_le()) };
            writer.write(&size_bytes)?;
            writer.write_all(value)?;
            writer.flush()?;
        }

        Ok(())
    }
}
