use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, error};

//use tracing_attributes::instrument;

use super::handler;
use super::recorder::{ConnectionRecorder, MasterRecorder};
use crate::memcache::store as storage;
use crate::protocol::binary_codec::{BinaryRequest, BinaryResponse};
use crate::protocol::binary_connection::MemcacheBinaryConnection;

pub struct ClientConfig {
    pub(crate) item_memory_limit: u32,
    pub(crate) rx_timeout_secs: u32,
    pub(crate) _wx_timeout_secs: u32,
}
pub struct Client {
    stream: MemcacheBinaryConnection,
    addr: SocketAddr,
    config: ClientConfig,
    handler: handler::BinaryHandler,
    recording: ConnectionRecorder,
}

impl Client {
    pub fn new(
        store: Arc<storage::MemcStore>,
        socket: TcpStream,
        addr: SocketAddr,
        config: ClientConfig,
        master_recorder: &Arc<MasterRecorder>,
    ) -> Self {
        let enable_recording = master_recorder.is_enabled();
        let connection_id = master_recorder.incr_conn_id();
        Client {
            stream: MemcacheBinaryConnection::new(socket, config.item_memory_limit),
            addr,
            config,
            handler: handler::BinaryHandler::new(store),
            recording: ConnectionRecorder::new(connection_id, enable_recording, master_recorder),
        }
    }

    pub async fn handle(&mut self) {
        debug!("New client connected: {}", self.addr);

        // Here for every packet we get back from the `Framed` decoder,
        // we parse the request, and if it's valid we generate a response
        // based on the values in the storage.
        loop {
            match timeout(
                Duration::from_secs(self.config.rx_timeout_secs as u64),
                self.stream.read_frame(),
            )
            .await
            {
                Ok(req_or_none) => {
                    let client_close = self.handle_frame(req_or_none).await;
                    if client_close {
                        return;
                    }
                }
                Err(err) => {
                    debug!(
                        "Timeout {}s elapsed, disconecting client: {}, error: {}",
                        self.config.rx_timeout_secs, self.addr, err
                    );
                    return;
                }
            }
        }
    }

    async fn handle_frame(&mut self, req: Result<Option<BinaryRequest>, io::Error>) -> bool {
        match req {
            Ok(re) => {
                match re {
                    Some(request) => self.handle_request(request).await,
                    None => {
                        // The connection will be closed at this point as `lines.next()` has returned `None`.
                        debug!("Connection closed: {}", self.addr);
                        self.recording.stop();
                        true
                    }
                }
            }
            Err(err) => {
                error!("Error when reading frame; error = {:?}", err);
                true
            }
        }
    }

    /// Handles single memcached binary request
    /// Returns true if we should leave client receive loop
    async fn handle_request(&mut self, request: BinaryRequest) -> bool {
        debug!("Got request {:?}", request.get_header());

        if let BinaryRequest::QuitQuietly(_req) = request {
            debug!("Closing client socket quit quietly");
            if let Err(_e) = self.stream.shutdown().await.map_err(log_error) {}
            return true;
        }

        self.recording.push_record(&request); // Record request and then replay
        let resp = self.handler.handle_request(request);
        match resp {
            Some(response) => {
                let mut socket_close = false;
                if let BinaryResponse::Quit(_resp) = &response {
                    socket_close = true;
                }

                debug!("Sending response {:?}", response);
                if let Err(e) = self.stream.write(&response).await {
                    error!("error on sending response; error = {:?}", e);
                    return true;
                }

                if socket_close {
                    debug!("Closing client socket quit command");
                    if let Err(_e) = self.stream.shutdown().await.map_err(log_error) {}
                    return true;
                }
                false
            }
            None => false,
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {}
}

fn log_error(e: io::Error) {
    // in most cases its not an error
    // client may just drop connection i.e. like
    // php client does
    if e.kind() == io::ErrorKind::NotConnected {
        info!("Error: {}", e);
    } else {
        error!("Error: {}", e);
    }
}
