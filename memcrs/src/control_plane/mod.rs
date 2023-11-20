use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::Future;
use http_body_util::Full;
use hyper::service::Service;
use tokio::net::TcpListener;

use hyper::server::conn::http1;
use hyper::{body::Incoming as IncomingBody, Response};
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use url::{form_urlencoded, Url};

use crate::memcache_server::recorder::MasterRecorder;

pub fn start_service(recorder: &Arc<MasterRecorder>) {
    let recorder = recorder.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .thread_name("Recorder")
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(start(&recorder))
    });
}

pub async fn start(
    recorder: &Arc<MasterRecorder>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], 11280));

    // We create a TcpListener and bind it to 127.0.0.1:11280
    let listener = TcpListener::bind(addr).await?;

    // We start a loop to continuously accept incoming connections
    loop {
        let recorder = recorder.clone();
        let (stream, _) = listener.accept().await?;

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io: TokioIo<_> = TokioIo::new(stream);

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(io, Svc { recorder })
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}

struct Svc {
    recorder: Arc<MasterRecorder>,
}

fn mk_response<'a>(s: &'a str) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let res = Bytes::from(s.to_string());
    Ok(Response::builder().body(Full::new(res)).unwrap())
}

impl Service<Request<IncomingBody>> for Svc {
    type Response = Response<Full<Bytes>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<IncomingBody>) -> Self::Future {
        let uri = req.uri();
        let path = uri.path();
        let method = req.method();
        let res = match (method, path) {
            (&Method::GET, "/") => mk_response(&format!("Memc benchmark control plan")),
            (&Method::POST, "/start-record") => self.start_record(),
            (&Method::POST, "/stop-record") => self.stop_record(&req),
            (&Method::POST, "/play-record") => todo!(),
            (&Method::POST, "/play-and-benchmark") => todo!(),
            // Return the 404 Not Found for other routes, and don't increment counter.
            _ => return Box::pin(async { mk_response("oh no! not found".into()) }),
        };
        Box::pin(async { res })
    }
}

impl Svc {
    fn start_record(&self) -> Result<Response<Full<Bytes>>, hyper::Error> {
        self.recorder.start();
        mk_response("ok")
    }
    fn stop_record(
        &self,
        req: &Request<IncomingBody>,
    ) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let query = get_params(req).unwrap();
        let name = query.get("name").unwrap();
        self.recorder.dump(name);
        mk_response("ok")
    }
}

fn get_params(req: &Request<IncomingBody>) -> Option<HashMap<String, String>> {
    req.uri().query().map(|q| {
        form_urlencoded::parse(q.as_bytes())
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect::<HashMap<_, _>>()
    })
}
