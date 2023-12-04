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
use url::form_urlencoded;

use crate::memcache::store::MemcStore;
use crate::memcache_server::recorder::MasterRecorder;

use self::playback_ctl::Playback;

mod playback_ctl;
mod runner;

pub fn start_service(recorder: &Arc<MasterRecorder>, store: &Arc<MemcStore>) {
    let recorder = recorder.clone();
    let store = store.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .thread_name("Recorder")
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(start(&recorder, &store))
    });
}

pub async fn start(
    recorder: &Arc<MasterRecorder>,
    store: &Arc<MemcStore>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], 11280));

    // We create a TcpListener and bind it to 127.0.0.1:11280
    let listener = TcpListener::bind(addr).await?;

    let recorder = recorder.clone();
    let store = store.clone();
    let inner = Arc::new(SvcInner {
        recorder,
        store,
        playback: Arc::new(Playback::new()),
    });
    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener.accept().await?;

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io: TokioIo<_> = TokioIo::new(stream);
        let inner = inner.clone();

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(
                    io,
                    Svc {
                        inner: inner.clone(),
                    },
                )
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}

struct Svc {
    inner: Arc<SvcInner>,
}

struct SvcInner {
    recorder: Arc<MasterRecorder>,
    playback: Arc<playback_ctl::Playback>,
    store: Arc<MemcStore>,
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
            (&Method::POST, "/play-record") => self.play_record(&req),
            (&Method::GET, "/playback-status") => self.playback_status(),
            // Return the 404 Not Found for other routes, and don't increment counter.
            _ => return Box::pin(async { mk_response("oh no! not found".into()) }),
        };
        Box::pin(async { res })
    }
}

impl Svc {
    fn start_record(&self) -> Result<Response<Full<Bytes>>, hyper::Error> {
        self.inner.recorder.start();
        mk_response(&format!("{}", self.inner.recorder.max_conn_id()))
    }
    fn stop_record(
        &self,
        req: &Request<IncomingBody>,
    ) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let query = get_params(req).unwrap();
        let name = query.get("name").unwrap();
        match self.inner.recorder.dump(name) {
            Ok(conns) => mk_response(&format!("{}/{}", conns, self.inner.recorder.max_conn_id())),
            Err(e) => mk_response(&e.to_string()),
        }
    }
    fn play_record(
        &self,
        req: &Request<IncomingBody>,
    ) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let query = get_params(req).unwrap();
        let name = query.get("name").unwrap();
        let start_res = self.inner.playback.start(name);
        let run_res =
            start_res && runner::run_records(&self.inner.playback, name, &self.inner.store);
        mk_response(&format!("{}", run_res))
    }
    fn playback_status(&self) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let res = self.inner.playback.status();
        let json = serde_json::to_string(&res).unwrap();
        mk_response(&format!("{}", json))
    }
}

fn get_params(req: &Request<IncomingBody>) -> Option<HashMap<String, String>> {
    req.uri().query().map(|q| {
        form_urlencoded::parse(q.as_bytes())
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect::<HashMap<_, _>>()
    })
}
