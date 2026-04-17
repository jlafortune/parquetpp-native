#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
APP_NAME="parquet++"
BUNDLE_DIR="$ROOT_DIR/dist/${APP_NAME}.app"
EXECUTABLE_NAME="parquetpp"
BIN_NAME="parquetpp-native"

cargo build --release --manifest-path "$ROOT_DIR/Cargo.toml"

rm -rf "$BUNDLE_DIR"
mkdir -p "$BUNDLE_DIR/Contents/MacOS" "$BUNDLE_DIR/Contents/Resources"

cp "$ROOT_DIR/macos/Info.plist" "$BUNDLE_DIR/Contents/Info.plist"
cp "$ROOT_DIR/macos/launcher.sh" "$BUNDLE_DIR/Contents/MacOS/$EXECUTABLE_NAME"
cp "$ROOT_DIR/target/release/$BIN_NAME" "$BUNDLE_DIR/Contents/Resources/$BIN_NAME"

chmod +x "$BUNDLE_DIR/Contents/MacOS/$EXECUTABLE_NAME"
chmod +x "$BUNDLE_DIR/Contents/Resources/$BIN_NAME"

echo "Created macOS app bundle:"
echo "  $BUNDLE_DIR"
