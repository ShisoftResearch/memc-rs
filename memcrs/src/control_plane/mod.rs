use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::Future;
use http_body_util::Full;
use hyper::service::Service;
use tokio::net::TcpListener;

use http_body_util::{combinators::BoxBody, BodyExt};
use hyper::body::Frame;
use hyper::server::conn::http1;
use hyper::{body::Incoming as IncomingBody, Response};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use url::Url;

use crate::cache::cache::Cache;

async fn start(
    storage: &Arc<dyn Cache + Send + Sync>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));

    // We create a TcpListener and bind it to 127.0.0.1:3000
    let listener = TcpListener::bind(addr).await?;

    // We start a loop to continuously accept incoming connections
    loop {
        let storage = storage.clone();
        let (stream, _) = listener.accept().await?;

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io: TokioIo<_> = TokioIo::new(stream);

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(io, Svc { storage })
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}

struct Svc {
    storage: Arc<dyn Cache + Send + Sync>,
}

fn mk_response(s: String) -> Result<Response<Full<Bytes>>, hyper::Error> {
    Ok(Response::builder().body(Full::new(Bytes::from(s))).unwrap())
}

impl Service<Request<IncomingBody>> for Svc {
    type Response = Response<Full<Bytes>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<IncomingBody>) -> Self::Future {
        let url = req.uri();
        let path = url.path();
        let method = req.method();
        let query = Url::parse(&url.to_string())
            .unwrap()
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>();
        let res = match (method, path) {
            (&Method::GET, "/") => mk_response(format!("Mmec benchmark control plan")),
            (&Method::POST, "/start-record") => todo!(),
            (&Method::POST, "/stop-record") => todo!(),
            (&Method::POST, "/play-record") => todo!(),
            (&Method::POST, "/play-and-benchmark") => todo!(),
            // Return the 404 Not Found for other routes, and don't increment counter.
            _ => return Box::pin(async { mk_response("oh no! not found".into()) }),
        };
        Box::pin(async { res })
    }
}

impl Svc {
    fn start_record(&self, query: &HashMap<String, String>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        unimplemented!()
    }
}
