#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${TARGET:-x86_64-unknown-linux-musl}"
BIN_NAME="mini-download-serve"
HOMEBREW_BIN="/opt/homebrew/bin"

cd "$ROOT_DIR"

if [[ -d "$HOMEBREW_BIN" ]]; then
  export PATH="$HOMEBREW_BIN:$PATH"
fi

echo "[build] target=$TARGET"

if command -v cargo-zigbuild >/dev/null 2>&1 && command -v zig >/dev/null 2>&1; then
  cargo zigbuild --release --target "$TARGET"
elif command -v cross >/dev/null 2>&1; then
  cross build --release --target "$TARGET"
else
  rustup target add "$TARGET"
  cargo build --release --target "$TARGET"
fi

echo "[build] binary ready: $ROOT_DIR/target/$TARGET/release/$BIN_NAME"
