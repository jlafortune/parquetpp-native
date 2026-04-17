mod app;
mod db;

use std::path::PathBuf;

use app::ParquetApp;

fn main() -> eframe::Result<()> {
    let native_options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([1500.0, 920.0])
            .with_min_inner_size([1000.0, 700.0])
            .with_title("parquet++"),
        ..Default::default()
    };

    let startup_path = std::env::args_os().nth(1).map(PathBuf::from);

    eframe::run_native(
        "parquet++",
        native_options,
        Box::new(move |_cc| Ok(Box::new(ParquetApp::new(startup_path.clone())))),
    )
}
