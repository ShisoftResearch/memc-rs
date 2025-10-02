use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io;
use tokio::net::TcpStream;
use tokio::time::timeout;
use log::{debug, error, info};

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
        let request_header = request.get_header().clone();
        debug!("Got request {:?}", request_header);

        if let BinaryRequest::QuitQuietly(_req) = request {
            debug!("Closing client socket quit quietly");
            if let Err(_e) = self.stream.shutdown().await.map_err(log_error) {}
            return true;
        }

        self.recording.push_record(&request); // Record request and then replay
        let (resp, _duration) = self.handler.handle_request(request);
        match resp {
            Some(response) => {
                let mut socket_close = false;
                if let BinaryResponse::Quit(_resp) = &response {
                    socket_close = true;
                }

                // Log operation results with detailed information
                self.log_operation_result(&request_header, &response);

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

    /// Logs detailed information about operation results, especially failures
    fn log_operation_result(&self, request_header: &crate::protocol::binary::RequestHeader, response: &BinaryResponse) {
        let response_header = response.get_header();
        let opcode_name = self.get_opcode_name(request_header.opcode);
        
        // Log successful operations at debug level
        if response_header.status == 0 {
            debug!("{} operation succeeded for client {} (opaque: {})", 
                   opcode_name, self.addr, request_header.opaque);
            return;
        }

        // Log failed operations with detailed error information
        let error_message = match response {
            BinaryResponse::Error(error_resp) => error_resp.error,
            _ => "Unknown error"
        };

        error!("{} operation FAILED for client {} (opaque: {}) - Status: 0x{:02x} ({}) - Error: {}", 
               opcode_name, self.addr, request_header.opaque, 
               response_header.status, self.get_status_name(response_header.status), error_message);

        // Log specific operation details for debugging
        match response {
            BinaryResponse::Set(_) | BinaryResponse::Add(_) | BinaryResponse::Replace(_) => {
                error!("Storage operation failed - Key length: {}, Value length: {}, CAS: 0x{:x}", 
                       request_header.key_length, request_header.body_length, request_header.cas);
            }
            BinaryResponse::Get(_) | BinaryResponse::GetKey(_) => {
                error!("Retrieval operation failed - Key length: {}", request_header.key_length);
            }
            BinaryResponse::Delete(_) => {
                error!("Delete operation failed - Key length: {}, CAS: 0x{:x}", 
                       request_header.key_length, request_header.cas);
            }
            _ => {}
        }
    }

    /// Maps opcode numbers to human-readable names
    fn get_opcode_name(&self, opcode: u8) -> &'static str {
        match opcode {
            0x00 => "GET",
            0x01 => "SET", 
            0x02 => "ADD",
            0x03 => "REPLACE",
            0x04 => "DELETE",
            0x05 => "INCREMENT",
            0x06 => "DECREMENT",
            0x07 => "QUIT",
            0x08 => "FLUSH",
            0x09 => "GET_QUIET",
            0x0a => "NOOP",
            0x0b => "VERSION",
            0x0c => "GET_KEY",
            0x0d => "GET_KEY_QUIET",
            0x0e => "APPEND",
            0x0f => "PREPEND",
            0x10 => "STAT",
            0x11 => "SET_QUIET",
            0x12 => "ADD_QUIET",
            0x13 => "REPLACE_QUIET",
            0x14 => "DELETE_QUIET",
            0x15 => "INCREMENT_QUIET",
            0x16 => "DECREMENT_QUIET",
            0x17 => "QUIT_QUIET",
            0x18 => "FLUSH_QUIET",
            0x19 => "APPEND_QUIET",
            0x1a => "PREPEND_QUIET",
            0x1c => "TOUCH",
            0x1d => "GET_AND_TOUCH",
            0x1e => "GET_AND_TOUCH_QUIET",
            0x20 => "SASL_LIST_MECHS",
            0x21 => "SASL_AUTH",
            0x22 => "SASL_STEP",
            0x23 => "GET_AND_TOUCH_KEY",
            0x24 => "GET_AND_TOUCH_KEY_QUIET",
            _ => "UNKNOWN"
        }
    }

    /// Maps status codes to human-readable names
    fn get_status_name(&self, status: u16) -> &'static str {
        match status {
            0x00 => "SUCCESS",
            0x01 => "KEY_NOT_EXISTS",
            0x02 => "KEY_EXISTS", 
            0x03 => "VALUE_TOO_LARGE",
            0x04 => "INVALID_ARGUMENTS",
            0x05 => "ITEM_NOT_STORED",
            0x06 => "NON_NUMERIC_VALUE",
            0x20 => "AUTHENTICATION_ERROR",
            0x21 => "AUTHENTICATION_CONTINUE",
            0x81 => "UNKNOWN_ERROR",
            0x82 => "OUT_OF_MEMORY",
            0x83 => "NOT_SUPPORTED",
            0x84 => "INTERNAL_ERROR",
            0x85 => "BUSY",
            0x86 => "TEMPORARY_FAILURE",
            _ => "UNKNOWN_STATUS"
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
