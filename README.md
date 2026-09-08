# mini-download-serve

用 Rust 写的迷你文件下载服务器：`axum` 路由 + `tokio` 异步 + `sendfile(2)` 零拷贝传输。
所有文件强制以 `Content-Disposition: attachment` 返回，浏览器访问即触发下载，类似
`python -m http.server`，但数据不经过用户态（page cache → socket，直接内核内搬运）。

## 用法

```sh
# 编译
cargo build --release

# 服务当前目录，默认端口 8000
./target/release/mini-download-serve

# 指定端口和目录
./target/release/mini-download-serve -p 9000 /path/to/files

# 指定监听地址（默认 0.0.0.0）
./target/release/mini-download-serve -b 127.0.0.1 -p 9000 /path/to/files
```

```
Usage: mini-download-serve [OPTIONS] [DIR]

Arguments:
  [DIR]  Directory to serve [default: current directory]

Options:
  -p, --port <PORT>   TCP port to listen on [default: 8000]
  -b, --bind <BIND>   Address to bind [default: 0.0.0.0]
  -h, --help          Print help
  -V, --version       Print version
```

## 特性

- **零拷贝**：文件数据通过 `sendfile(2)` 从页缓存直送 socket，不进用户态、不占
  worker 线程 CPU；socket 缓冲写满时通过 `AsyncFd` 异步等待可写。
- **强制下载**：每个文件都带 `attachment` 的 `Content-Disposition`（含 RFC 5987
  `filename*`，中文等非 ASCII 文件名在浏览器里正常保存）。
- **Range 请求**：支持 `bytes=start-end` / `start-` / `-suffix` 单区间断点续传，
  越界返回 `416`，多区间请求回退为整文件。
- **目录索引**：访问目录返回简单 HTML 列表（目录优先排序、大小、修改时间），
  无尾斜杠时 `301` 补斜杠，风格与 `python -m http.server` 一致。
- **路径安全**：拒绝 `..` 穿越，symlink 解析后必须仍在根目录内，否则 `403`。
- **HEAD** 支持，便于探测文件大小。

## 实现说明

由于 hyper 的响应编码器不知道 sendfile 直写了多少字节，一个下载响应完成后
连接会以 `Connection: close` 关闭（文件字节此时已全部进入内核发送缓冲，
客户端可完整收下——已用 md5 验证）。目录页/错误页等普通响应不受影响。

代码结构：

- `src/main.rs` — CLI（clap）、accept 循环、每连接一个 hyper http1 task
- `src/conn.rs` — 自定义 hyper IO 层：响应头屏障（marker 流式匹配，确保
  headers 先于 sendfile 到达 socket）+ `AsyncFd` 可写等待
- `src/sendfile.rs` — macOS/Linux 两个 `sendfile` 封装与 `http_body::Body` 实现
- `src/web.rs` — axum 路由、下载 handler、Range 解析、目录索引

平台支持：macOS 与 Linux（其余平台编译期报错）。
