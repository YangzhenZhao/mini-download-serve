//! HTTP surface: routing, the download handler (forced `attachment`), range
//! support and a directory index in the spirit of `python -m http.server`.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use chrono::{DateTime, Local};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

use crate::conn::{next_marker_value, ConnShared, MARKER_HEADER};
use crate::sendfile::SendfileBody;

// Per-connection shared state (socket fd + header barrier), injected into
// the connection task from the accept loop in main().
tokio::task_local! {
    pub(crate) static CONN: Arc<ConnShared>;
}

/// Canonicalized serving root.
pub(crate) struct Root {
    pub(crate) path: PathBuf,
}

pub(crate) fn router(root: Arc<Root>) -> Router {
    Router::new()
        .route("/", get(entry).head(entry))
        .route("/{*rest}", get(entry).head(entry))
        .with_state(root)
}

#[axum::debug_handler]
async fn entry(State(root): State<Arc<Root>>, req: Request) -> Response {
    let uri_path = req.uri().path().to_string();
    // url-decoded by axum's Path extractor in routing; here we decode the
    // raw uri path ourselves (axum matched on the raw path)
    let rel = percent_decode(&uri_path);

    // reject traversal early
    if rel.split('/').any(|seg| seg == ".." || seg.contains('\0')) {
        return error_page(StatusCode::FORBIDDEN, "403 Forbidden");
    }

    let target = root.path.join(rel.trim_start_matches('/'));
    let real = match tokio::fs::canonicalize(&target).await {
        Ok(p) => p,
        Err(_) => return error_page(StatusCode::NOT_FOUND, "404 Not Found"),
    };
    // symlink or traversal escaping the root is not served
    if !real.starts_with(&root.path) {
        return error_page(StatusCode::FORBIDDEN, "403 Forbidden");
    }

    let meta = match tokio::fs::metadata(&real).await {
        Ok(m) => m,
        Err(_) => return error_page(StatusCode::NOT_FOUND, "404 Not Found"),
    };

    if meta.is_dir() {
        if !uri_path.ends_with('/') {
            // keep relative links working, like python's http.server
            let loc = format!("{}/", uri_path.trim_end_matches('?'));
            return Response::builder()
                .status(StatusCode::MOVED_PERMANENTLY)
                .header(header::LOCATION, loc)
                .body(Body::empty())
                .unwrap();
        }
        return match list_dir(&real, &uri_path).await {
            Ok(html) => Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
                .body(Body::from(html))
                .unwrap(),
            Err(_) => error_page(StatusCode::INTERNAL_SERVER_ERROR, "500 Internal Server Error"),
        };
    }

    if !meta.is_file() {
        return error_page(StatusCode::NOT_FOUND, "404 Not Found");
    }

    serve_file(real, req, meta.len()).await
}

async fn serve_file(real: PathBuf, req: Request, total: u64) -> Response {
    let shared = CONN.get().clone();
    let marker_value = next_marker_value();
    shared.arm(marker_value);

    let file = match tokio::fs::File::open(&real).await {
        Ok(f) => match f.try_into_std() {
            // a freshly opened file has no in-flight async op, this can't fail
            Ok(f) => f,
            Err(_) => return error_page(StatusCode::INTERNAL_SERVER_ERROR, "500 Internal Server Error"),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return error_page(StatusCode::NOT_FOUND, "404 Not Found")
        }
        Err(_) => return error_page(StatusCode::INTERNAL_SERVER_ERROR, "500 Internal Server Error"),
    };

    let name = real
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "download".to_string());

    let mime = mime_guess::from_path(real)
        .first_raw()
        .unwrap_or("application/octet-stream");

    // range handling (single range only; anything else falls back to 200)
    let range_hdr = req.headers().get(header::RANGE).and_then(|v| v.to_str().ok());
    let (status, offset, len, content_range) = match parse_range(range_hdr, total) {
        RangeKind::Full => (StatusCode::OK, 0, total, None),
        RangeKind::Part(off, l) => (
            StatusCode::PARTIAL_CONTENT,
            off,
            l,
            Some(format!("bytes {}-{}/{}", off, off + l - 1, total)),
        ),
        RangeKind::Unsatisfiable => {
            return Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::CONTENT_RANGE, format!("bytes */{total}"))
                .body(Body::empty())
                .unwrap()
        }
    };

    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, mime)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, len.to_string())
        .header(header::CONTENT_DISPOSITION, content_disposition(&name))
        .header(MARKER_HEADER, marker_value.to_string());

    if let Some(cr) = content_range {
        builder = builder.header(header::CONTENT_RANGE, cr);
    }

    if req.method() == Method::HEAD {
        return builder.body(Body::empty()).unwrap();
    }

    // The sendfile path ends with hyper noticing it wrote none of the
    // advertised body itself and closing the connection — tell the client
    // up front so the semantics are clean.
    builder = builder.header(header::CONNECTION, "close");

    builder
        .body(Body::new(SendfileBody::new(file, shared, offset, len)))
        .unwrap()
}

enum RangeKind {
    Full,
    Part(u64, u64),
    Unsatisfiable,
}

fn parse_range(hdr: Option<&str>, total: u64) -> RangeKind {
    let Some(spec) = hdr.and_then(|h| h.strip_prefix("bytes=")) else {
        return RangeKind::Full;
    };
    if spec.contains(',') {
        return RangeKind::Full; // multipart ranges unsupported → full body
    }
    let spec = spec.trim();
    let (start_s, end_s) = match spec.split_once('-') {
        Some(x) => x,
        None => return RangeKind::Full,
    };

    // suffix form: "bytes=-N" (last N bytes)
    if start_s.is_empty() {
        let Ok(n) = end_s.parse::<u64>() else {
            return RangeKind::Full;
        };
        if n == 0 {
            return RangeKind::Full;
        }
        let len = n.min(total);
        return RangeKind::Part(total - len, len);
    }

    let Ok(start) = start_s.trim().parse::<u64>() else {
        return RangeKind::Full;
    };
    if start >= total {
        return RangeKind::Unsatisfiable;
    }
    let end = match end_s.trim() {
        "" => total - 1, // "bytes=N-"
        s => match s.parse::<u64>() {
            Ok(e) => e.min(total - 1),
            Err(_) => return RangeKind::Full,
        },
    };
    if start > end {
        return RangeKind::Full;
    }
    RangeKind::Part(start, end - start + 1)
}

/// `attachment; filename="ascii"; filename*=UTF-8''percent-encoded`
fn content_disposition(name: &str) -> HeaderValue {
    let encoded = utf8_percent_encode(name, DISPOSITION_SET).to_string();
    let fallback: String = name
        .chars()
        .map(|c| {
            if c.is_ascii() && !c.is_control() && c != '"' && c != '\\' {
                c
            } else {
                '_'
            }
        })
        .collect();
    HeaderValue::from_str(&format!(
        "attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"
    ))
    .unwrap()
}

const DISPOSITION_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'%')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'{')
    .add(b'}');

const HREF_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}');

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

async fn list_dir(real: &Path, url_path: &str) -> io::Result<String> {
    let mut rd = tokio::fs::read_dir(real).await?;
    let mut entries: Vec<(String, bool, u64, Option<std::time::SystemTime>)> = Vec::new();
    while let Some(e) = rd.next_entry().await? {
        let name = e.file_name().to_string_lossy().into_owned();
        let meta = match e.metadata().await {
            Ok(m) => m,
            Err(_) => {
                entries.push((name, false, 0, None));
                continue;
            }
        };
        let mtime = meta.modified().ok();
        entries.push((name, meta.is_dir(), meta.len(), mtime));
    }
    entries.sort_by(|a, b| match (a.1, b.1) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.0.cmp(&b.0),
    });

    let title = html_escape(url_path);
    let mut rows = String::new();
    if url_path != "/" {
        rows.push_str("<li><a href=\"../\">../</a></li>\n");
    }
    for (name, is_dir, size, mtime) in entries {
        let href = utf8_percent_encode(&name, HREF_SET).to_string();
        let disp = html_escape(&name);
        let kind = if is_dir { "dir" } else { "file" };
        let size = if is_dir { "-".into() } else { human_size(size) };
        let mtime = mtime
            .map(|t| {
                DateTime::<Local>::from(t)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string()
            })
            .unwrap_or_else(|| "-".into());
        rows.push_str(&format!(
            "<li><a href=\"{href}{suffix}\">{disp}</a> <small>[{kind}, {size}, {mtime}]</small></li>\n",
            suffix = if is_dir { "/" } else { "" },
        ));
    }

    Ok(format!(
        "<!DOCTYPE html>\n<html>\n<head><meta charset=\"utf-8\"><title>Index of {title}</title>\
<style>body{{font-family:system-ui,sans-serif;max-width:48em;margin:2em auto;padding:0 1em}}\
li{{line-height:1.8;list-style:none}}small{{color:#777}}</style></head>\n\
<body><h1>Index of {title}</h1><hr><ul>\n{rows}</ul><hr>\
<p style=\"color:#777\">mini-download-serve — files are served as downloads (attachment)</p>\
</body>\n</html>\n"
    ))
}

fn human_size(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut n = n as f64;
    let mut u = 0;
    while n >= 1024.0 && u < UNITS.len() - 1 {
        n /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n:.0} B")
    } else {
        format!("{n:.1} {}", UNITS[u])
    }
}

fn error_page(status: StatusCode, msg: &str) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(format!("{msg}\n")))
        .unwrap()
}

fn percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}
