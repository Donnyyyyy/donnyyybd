use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    mem::transmute,
};

use super::Storage;

const KEY_SIZE: usize = 128;

pub struct LogStorage<'a> {
    reader: BufReader<&'a File>,
    writer: BufWriter<&'a File>,
}

impl LogStorage<'_> {
    pub fn new<'a>(fin: &'a File, fout: &'a File) -> LogStorage<'a> {
        LogStorage {
            reader: BufReader::new(&fin),
            writer: BufWriter::new(&fout),
        }
    }

    fn compare_keys(a: &[u8], b: &[u8]) -> bool {
        if a.len() < KEY_SIZE {
            if b[a.len()] != 0 {
                return false;
            }
        }

        for i in 0..a.len() {
            if a[i] != b[i] {
                return false;
            }
        }
        true
    }
}

impl Storage for LogStorage<'_> {
    fn get(&mut self, key: String) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let target_key_bytes = key.as_bytes();

        self.reader.seek(io::SeekFrom::Start(0))?;

        let mut key_buf = [0; KEY_SIZE];
        let mut size_buf: [u8; 8] = [0; 8];

        loop {
            match self.reader.read_exact(&mut key_buf) {
                Ok(_) => {}
                Err(error) => match error.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        return Ok(None);
                    },
                    _ => {
                        return Err(Box::new(error));
                    },
                },
            };
            self.reader.read_exact(&mut size_buf)?;
            let value_size = usize::from_ne_bytes(size_buf);

            if Self::compare_keys(target_key_bytes, &key_buf) {
                let mut value = vec![0u8; value_size];
                self.reader.read_exact(&mut value)?;
                return Ok(Some(value));
            }

            self.reader.seek_relative(value_size.try_into()?)?;
        }
    }

    fn set(
        &mut self,
        key: String,
        value: &[u8],
        size: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key_bytes = key.as_bytes();
        self.writer.write_all(key_bytes)?;

        for _ in 0..KEY_SIZE - key_bytes.len() {
            self.writer.write(&[0])?;
        }

        let size_bytes: [u8; 8] = unsafe { transmute(size.to_le()) };
        self.writer.write_all(value)?;
        self.writer.write(&size_bytes)?;
        self.writer.flush()?;

        Ok(())
    }
}
