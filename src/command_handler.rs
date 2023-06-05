use std::{
    error, fmt,
    io::{BufRead, BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream},
    str::from_utf8,
    thread::sleep,
    time::{Duration, SystemTime},
};

use crate::storage::Storage;

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

#[derive(Debug, Clone)]
struct InvalidMsgError;

impl fmt::Display for InvalidMsgError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "msg invalid")
    }
}

impl error::Error for InvalidMsgError {}

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

        let mut command = String::new();
        let mut size;
        loop {
            size = self.reader.read_line(&mut command)?;
            if size != 0 {
                break;
            }
            if start.elapsed()?.as_millis() > timeout_ms {
                println!("commant timeout");
                return Ok(Command::Unknown);
            }
            sleep(POLL_INTERVAL)
        }
        println!("command '{}' ({} bytes)", command, size);

        if command == "GET\n" {
            return Ok(Command::Get);
        } else if command == "SET\n" {
            return Ok(Command::Set);
        }

        Ok(Command::Unknown)
    }

    fn handle_get(&mut self) -> Result<(), Box<dyn error::Error>> {
        let mut key = Vec::new();
        self.reader.read_until(0, &mut key).unwrap();
        let key_str = from_utf8(&key[0..key.len() - 1])?;

        println!("GET '{}'", key_str);
        match self.storage.get(key_str.to_string()) {
            Ok(val) => match val {
                Some(value) => {
                    self.writer.write_all(&value)?;
                    self.writer.write_all(&[0])?;
                    self.writer.flush()?;
                }
                None => {
                    self.writer.write_all(&[0])?;
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
        let mut msg: Vec<u8> = Vec::new();
        self.reader.read_until(0, &mut msg)?;

        if msg[msg.len() - 1] != 0 {
            return Err(InvalidMsgError.into());
        }

        let mut sep_idx: usize = 0;
        for (i, v) in msg.iter().enumerate() {
            if *v == b'\n' {
                sep_idx = i;
            }
        }
        if sep_idx == 0 {
            return Err(InvalidMsgError.into());
        }
        let key = from_utf8(&msg[0..sep_idx])?;
        let value = &msg[sep_idx + 1..msg.len() - 1];

        println!("SET '{}', '{}'", key, from_utf8(value)?);
        match self.storage.set(key.to_string(), value, value.len()) {
            Ok(_) => {
                self.writer.write_all(&[79, 75, 0])?;
                self.writer.flush()?;
            }
            Err(error) => {
                return Err(error);
            }
        }

        Ok(())
    }
}
