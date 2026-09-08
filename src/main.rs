//! mini-download-serve — a tiny zero-copy file download server.
//!
//! Serves every file as a forced download (`Content-Disposition:
//! attachment`) using axum for routing and sendfile(2) for the payload:
//! bytes move from the page cache straight into the socket buffer without
//! ever crossing into userspace.
//!
//! Usage: mini-download-serve [-p PORT] [-b ADDR] [DIR]

mod conn;
mod sendfile;
mod web;

use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use hyper_util::service::TowerToHyperService;

/// Serve a directory for download, like `python -m http.server` but every
/// file is forced to download (attachment) and transfers use sendfile
/// zero-copy.
#[derive(Parser)]
#[command(name = "mini-download-serve", version = env!("CARGO_PKG_VERSION"))]
struct Args {
    /// TCP port to listen on
    #[arg(short = 'p', long = "port", default_value_t = 8000)]
    port: u16,

    /// Address to bind
    #[arg(short = 'b', long = "bind", default_value = "0.0.0.0")]
    bind: String,

    /// Directory to serve [default: current directory]
    dir: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    let serve_dir = args.dir.clone().unwrap_or_else(|| ".".into());
    let dir = match serve_dir.canonicalize() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: cannot access directory {:?}: {e}", serve_dir);
            return ExitCode::FAILURE;
        }
    };
    if !dir.is_dir() {
        eprintln!("error: {} is not a directory", dir.display());
        return ExitCode::FAILURE;
    }

    let root = Arc::new(web::Root { path: dir.clone() });
    let service = TowerToHyperService::new(web::router(root));

    let listener = match tokio::net::TcpListener::bind((args.bind.as_str(), args.port)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("error: cannot bind {}:{}: {e}", args.bind, args.port);
            return ExitCode::FAILURE;
        }
    };

    let display_addr = if args.bind == "0.0.0.0" || args.bind == "::" {
        format!("127.0.0.1:{}", args.port)
    } else {
        format!("{}:{}", args.bind, args.port)
    };
    println!(
        "Serving downloads from {} at http://{}/ — press Ctrl+C to stop",
        dir.display(),
        display_addr
    );

    loop {
        let (stream, _peer) = tokio::select! {
            res = listener.accept() => match res {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("accept error: {e}");
                    continue;
                }
            },
            _ = tokio::signal::ctrl_c() => {
                println!("shutting down");
                return ExitCode::SUCCESS;
            }
        };
        let _ = stream.set_nodelay(true);

        let (io, shared) = match conn::ConnIo::new(stream) {
            Ok(x) => x,
            Err(_) => continue,
        };

        let service = service.clone();
        tokio::spawn(web::CONN.scope(shared, async move {
            let res = hyper::server::conn::http1::Builder::new()
                .keep_alive(true)
                .serve_connection(io, service)
                .await;
            // A finished sendfile response makes hyper report "body write
            // aborted" (it wrote none of the advertised bytes itself) and
            // close the connection — the file is already fully in the
            // kernel socket buffer at that point, so this is expected and
            // harmless. Real transport errors end up here too; there is
            // nothing worth logging about either.
            if let Err(_e) = res {
                // intentionally quiet
            }
        }));
    }
}
