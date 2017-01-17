extern crate futures;
extern crate futures_cpupool;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;

use std::io;
use std::str;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio_core::io::{Codec, EasyBuf};
use tokio_proto::pipeline::ServerProto;
use tokio_core::io::{Io, Framed};
use tokio_service::Service;
use tokio_proto::TcpServer;
use futures::{future, Async, Future, BoxFuture};
use futures_cpupool::CpuPool;

pub struct LineCodec;

pub enum Command {
    Get(String),
    Set(String, String),
}

impl Codec for LineCodec {
    type In = Command;
    type Out = String;

    fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<Self::In>> {
        if let Some(i) = buf.as_slice().iter().position(|&b| b == b'\n') {
            // remove the serialized frame from the buffer.
            let line = buf.drain_to(i);

            // Also remove the '\n'
            buf.drain_to(1);

            let fields = str::from_utf8(&line.as_ref()).unwrap().split(" ").collect::<Vec<&str>>();
            if fields.len() < 1 {
                return Err(io::Error::new(io::ErrorKind::Other, "missing command"));
            }

            return match fields[0] {
                "get" => {
                    if fields.len() < 2 {
                        Err(io::Error::new(io::ErrorKind::Other, "get: missing key"))
                    } else {
                        Ok(Some(Command::Get(fields[1].to_string())))
                    }
                }
                "set" => {
                    if fields.len() < 3 {
                        Err(io::Error::new(io::ErrorKind::Other, "set: missing key and/or value"))
                    } else {
                        Ok(Some(Command::Set(fields[1].to_string(), fields[2].to_string())))
                    }
                }
                _ => {
                    Err(io::Error::new(io::ErrorKind::Other,
                                       format!("unknown command: {}", fields[0])))
                }
            };
        } else {
            Ok(None)
        }
    }

    fn encode(&mut self, msg: String, buf: &mut Vec<u8>) -> io::Result<()> {
        buf.extend_from_slice(msg.as_bytes());
        buf.push(b'\n');
        Ok(())
    }
}

pub struct LineProto;

impl<T: Io + 'static> ServerProto<T> for LineProto {
    /// For this protocol style, `Request` matches the codec `In` type
    type Request = Command;

    /// For this protocol style, `Response` matches the coded `Out` type
    type Response = String;

    /// A bit of boilerplate to hook in the codec:
    type Transport = Framed<T, LineCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;
    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(LineCodec))
    }
}

pub struct KV {
    store: Arc<Mutex<HashMap<String, String>>>,
    thread_pool: CpuPool,
}

impl KV {
    fn new() -> KV {
        KV {
            store: Arc::new(Mutex::new(HashMap::new())),
            thread_pool: CpuPool::new(10),
        }
    }
}

impl Service for KV {
    // These types must match the corresponding protocol types:
    type Request = Command;
    type Response = String;

    // For non-streaming protocols, service errors are always io::Error
    type Error = io::Error;

    // The future for computing the response; box it for simplicity.
    type Future = BoxFuture<Self::Response, Self::Error>;

    // Produce a future for computing a response from a request.
    fn call(&self, req: Self::Request) -> Self::Future {
        let mut store = self.store.lock().unwrap();
        let res = match req {
            Command::Get(ref k) => {
                match store.get(k) {
                    Some(v) => v.to_string(),
                    None => "".to_string(),
                }
            }
            Command::Set(k, v) => {
                match store.insert(k, v) {
                    Some(v_old) => v_old.to_string(),
                    None => "".to_string(),
                }
            }
        };
        future::ok(res).boxed()
    }
}

fn main() {
    // Specify the localhost address
    let addr = "0.0.0.0:12345".parse().unwrap();

    // The builder requires a protocol and an address
    let server = TcpServer::new(LineProto, addr);

    // We provide a way to *instantiate* the service for each new
    // connection; here, we just immediately return a new instance.
    server.serve(|| Ok(KV::new()));
}
