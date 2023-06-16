use std::{
    error,
    io::{ BufReader, BufWriter, Read, Write, ErrorKind},
    mem::transmute,
    net::{TcpListener, TcpStream},
    str::from_utf8,
    sync::{Arc, Mutex},
    thread::{self, sleep, JoinHandle},
    time::{Duration, SystemTime},
};

use log::{info, warn};

use crate::storage::StorageClient;


const OK: [u8; 3] = [79, 75, 0];

pub struct DbServer {
    listener: TcpListener,
}

enum Command {
    Get,
    Set,
    Unknown,
}

impl DbServer {
    pub fn new(host: String, port: u16) -> Result<DbServer, Box<dyn error::Error>> {
        let listener = TcpListener::bind(format!("{}:{}", host, port)).unwrap();
        Ok(DbServer { listener: listener })
    }

    pub fn start(self) -> Result<(), Box<dyn error::Error>> {
        info!("Listening");
        let mut threads: Vec<JoinHandle<()>> = Vec::new();

        let storage_writer_mtx = Arc::new(Mutex::new(StorageClient::new_writer()?));

        for stream in self.listener.incoming() {
            match stream {
                Ok(tcp_stream) => {
                    let writer_arc_clone = Arc::clone(&storage_writer_mtx);
                    threads.push(thread::spawn(move || {
                        ClientHandler::handle(&tcp_stream, StorageClient::new(writer_arc_clone).unwrap());
                    }));
                }
                Err(err) => {
                    warn!("failed to handle connection: {}", err)
                }
            }
        }

        for thread in threads {
            thread.join().unwrap();
        }

        Ok(())
    }
}

struct ClientHandler<'a> {
    storage: StorageClient,
    writer: BufWriter<&'a TcpStream>,
    reader: BufReader<&'a TcpStream>,
}

impl ClientHandler<'_> {
    fn handle<'a>(stream: &'a TcpStream, storage: StorageClient) {
        info!("new conn");

        let mut handler = ClientHandler {
            storage,
            writer: BufWriter::new(&stream),
            reader: BufReader::new(&stream),
        };
        handler.handle_loop();
    }

    fn handle_loop(&mut self) {
        loop {
            match self.parse_command() {
                Ok(command) => match command {
                    Command::Get => match self.handle_get() {
                        Ok(_) => {}
                        Err(err) => {
                            warn!("error {}", err);
                            break;
                        }
                    },
                    Command::Set => match self.handle_set() {
                        Ok(_) => {}
                        Err(err) => {
                            warn!("error {}", err);
                            break;
                        }
                    },
                    Command::Unknown => {
                        break;
                    }
                },
                Err(_) => {
                    warn!("error reading command");
                    break;
                }
            }
        }
        info!("client disconnected");
    }

    fn parse_command(&mut self) -> Result<Command, Box<dyn error::Error>> {
        const POLL_INTERVAL: Duration = Duration::from_millis(50);

        let timeout_ms = 2000;
        let start = SystemTime::now();

        let mut command_buf: [u8; 3] = [0; 3];
        loop {
            match self.reader.read_exact(&mut command_buf) {
                Ok(_) => break,
                Err(error) => match error.kind() {
                    ErrorKind::UnexpectedEof => {}
                    _ => {
                        return Err(Box::new(error));
                    }
                },
            };

            if start.elapsed()?.as_millis() > timeout_ms {
                info!("command timeout");
                return Ok(Command::Unknown);
            }
            sleep(POLL_INTERVAL)
        }
        let command = from_utf8(&command_buf)?;

        if command == "GET" {
            return Ok(Command::Get);
        } else if command == "SET" {
            return Ok(Command::Set);
        }

        Ok(Command::Unknown)
    }

    fn read_varsize(&mut self) -> Result<Vec<u8>, Box<dyn error::Error>> {
        let mut size_buf: [u8; 8] = [0; 8];
        self.reader.read_exact(&mut size_buf)?;

        let mut value = vec![0u8; usize::from_le_bytes(size_buf)];
        self.reader.read_exact(&mut value)?;

        return Ok(value);
    }

    fn handle_get(&mut self) -> Result<(), Box<dyn error::Error>> {
        let key_vec = self.read_varsize()?;
        let key = from_utf8(&key_vec)?.to_string();

        info!("GET '{}'", key);
        match self.storage.get(key) {
            Ok(val) => match val {
                Some(value) => {
                    let size_bytes: [u8; 8] = unsafe { transmute(value.len().to_le()) };
                    self.writer.write_all(&size_bytes)?;
                    self.writer.write_all(&value)?;
                    self.writer.flush()?;
                }
                None => {
                    self.writer.write_all(&[0; 8])?;
                    self.writer.flush()?;
                }
            },
            Err(error) => {
                return Err(error);
            }
        }
        Ok(())
    }

    fn handle_set(&mut self) -> Result<(), Box<dyn error::Error>> {
        let key_vec = self.read_varsize()?;
        let key = from_utf8(&key_vec)?.to_string();

        let value = self.read_varsize()?;

        info!("SET '{}', {} bytes", key, value.len());
        match self.storage.set(key, &value, value.len()) {
            Ok(_) => {
                self.writer.write_all(&OK)?;
                self.writer.flush()?;
            }
            Err(error) => {
                return Err(error);
            }
        }

        Ok(())
    }
}
