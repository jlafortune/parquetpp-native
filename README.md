# parquet++ native

This folder contains a native macOS desktop implementation of `parquet++` built with:

- Rust
- `egui` / `eframe`
- embedded DuckDB

## Current capabilities

- Open local `.parquet` files
- Fast paged browsing for larger datasets
- Column sorting
- Multi-filter queries
- Optional SQL query mode
- Row selection
- Cell selection
- Inline edit mode
- `Save` back to the opened parquet file
- `Save As` to a new parquet file

## Run locally on macOS

From the repo root:

```bash
cargo run --manifest-path native/parquetpp-native/Cargo.toml -- sample-data/people-small.parquet
```

Or start it without a file and use the `Open File` button:

```bash
cargo run --manifest-path native/parquetpp-native/Cargo.toml
```

## Build a release binary

```bash
cargo build --release --manifest-path native/parquetpp-native/Cargo.toml
```

The binary will be at:

`native/parquetpp-native/target/release/parquetpp-native`

## Build a macOS app bundle

```bash
bash native/parquetpp-native/scripts/package_macos_app.sh
```

This creates:

`native/parquetpp-native/dist/parquet++.app`

You can drag that app into `/Applications`.

## Set as the default parquet opener on macOS

1. Build the app bundle.
2. Drag `parquet++.app` into `/Applications`.
3. Open it once from `/Applications`.
4. In Finder, select any `.parquet` file and choose `Get Info`.
5. Under `Open with`, choose `parquet++`.
6. Click `Change All...` to make it the default opener.

## Notes

- Packaging, signing, notarization, and native file-handler registration are intentionally left for later.
- `Cmd+C` copies the currently selected cell.
- `Cmd+S` saves while in edit mode.
- SQL syntax: use a `SELECT` or `WITH` query against the virtual table named `data`, for example `SELECT * FROM data WHERE country = 'US'`.
