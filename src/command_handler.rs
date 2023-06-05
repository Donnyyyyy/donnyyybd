use std::{
    error,
    io::{self, BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
    str::from_utf8,
    thread::sleep,
    time::{Duration, SystemTime}, mem::transmute,
};

use crate::storage::Storage;

const OK: [u8; 3] = [79, 75, 0];

pub struct DbServer<'a> {
    listener: TcpListener,
    storage: &'a mut dyn Storage,
}

enum Command {
    Get,
    Set,
    Unknown,
}

impl DbServer<'_> {
    pub fn start(
        host: String,
        port: u16,
        storage: &mut dyn Storage,
    ) -> Result<DbServer, Box<dyn error::Error>> {
        let listener = TcpListener::bind(format!("{}:{}", host, port)).unwrap();
        println!("Listening");
        let mut server = DbServer {
            listener: listener,
            storage,
        };
        server.handle_incoming_connections()?;
        Ok(server)
    }

    fn handle_incoming_connections(&mut self) -> Result<(), Box<dyn error::Error>> {
        for stream in self.listener.incoming() {
            StreamHandler::handle(stream?, self.storage)?;
        }
        Ok(())
    }
}

struct StreamHandler<'a> {
    storage: &'a mut dyn Storage,
    writer: BufWriter<&'a TcpStream>,
    reader: BufReader<&'a TcpStream>,
}

impl StreamHandler<'_> {
    fn handle(stream: TcpStream, storage: &mut dyn Storage) -> Result<(), Box<dyn error::Error>> {
        println!("new conn");
        stream.set_read_timeout(Some(Duration::from_millis(3000)))?;

        StreamHandler {
            storage,
            writer: BufWriter::new(&stream),
            reader: BufReader::new(&stream),
        }
        .handle_loop();
        Ok(())
    }

    fn handle_loop(&mut self) {
        loop {
            match self.parse_command() {
                Ok(command) => match command {
                    Command::Get => match self.handle_get() {
                        Ok(_) => {}
                        Err(err) => {
                            println!("error {}", err);
                            break;
                        }
                    },
                    Command::Set => match self.handle_set() {
                        Ok(_) => {}
                        Err(err) => {
                            println!("error {}", err);
                            break;
                        }
                    },
                    Command::Unknown => {
                        break;
                    }
                },
                Err(_) => {
                    println!("error reading command");
                    break;
                }
            }
        }
        println!("client disconnected");
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
                    io::ErrorKind::UnexpectedEof => {}
                    _ => {
                        return Err(Box::new(error));
                    }
                },
            };

            if start.elapsed()?.as_millis() > timeout_ms {
                println!("commant timeout");
                return Ok(Command::Unknown);
            }
            sleep(POLL_INTERVAL)
        }
        let command = from_utf8(&command_buf)?;
        println!("command '{}'", command);

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

        println!("GET '{}'", key);
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

        println!("SET '{}', {} bytes", key, value.len());
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
