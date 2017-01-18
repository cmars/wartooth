extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;

use std::io;
use std::str;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_core::io::{Codec, EasyBuf};
use tokio_core::reactor::{Core, Timeout};
use tokio_core::net::TcpListener;
use tokio_proto::pipeline::ServerProto;
use tokio_core::io::{Io, Framed};
use tokio_service::Service;
use tokio_proto::TcpServer;
use futures::{future, Future, BoxFuture, Stream, Sink};

pub struct LineCodec;

pub enum Command {
    Get(String),
    Set(String, String),
    Quit,
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
                "quit" => Ok(Some(Command::Quit)),
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
        if !msg.is_empty() {
            buf.extend_from_slice(msg.as_bytes());
            buf.push(b'\n');
        }
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
}

impl KV {
    fn new(store: Arc<Mutex<HashMap<String, String>>>) -> KV {
        KV { store: store }
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
            Command::Quit => {
                println!("quit received");
                return future::err(io::Error::new(io::ErrorKind::Other, "quit")).boxed();
            }
        };
        future::ok(res).boxed()
    }
}

fn main() {
    let store = Arc::new(Mutex::new(HashMap::new()));

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let addr = "0.0.0.0:12345".parse().unwrap();
    let listener = TcpListener::bind(&addr, &handle).unwrap();
    let connections = listener.incoming();
    let timebomb = Timeout::new(Duration::new(30, 0), &handle).unwrap();
    let server = connections.for_each(move |(socket, _peer_addr)| {
            let (wr, rd) = socket.framed(LineCodec).split();
            let mut service = KV::new(store.clone());

            let responses = rd.and_then(move |req| service.call(req));
            let server = wr.send_all(responses).then(|_| Ok(()));
            handle.spawn(server);

            Ok(())
        })
        .select(timebomb);

    core.run(server);
}
