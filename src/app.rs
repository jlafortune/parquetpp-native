use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, Sender, unbounded};
use eframe::egui::{
    self, Align, Button, Color32, ComboBox, Context, CornerRadius, Frame, KeyboardShortcut, Key, Layout, Modifiers,
    RichText, ScrollArea, Sense, Stroke, TextEdit, Ui, Vec2,
};
use egui_extras::{Column, TableBuilder};
use rfd::FileDialog;

use crate::db::{
    CellValue, ColumnCategory, ColumnSchema, DatasetSnapshot, FilterOperator, FilterRule, QuerySpec, RowData, SortDirection,
    SortSpec, WorkerCommand, WorkerEvent, spawn_worker,
};

const PAGE_SIZE_ALL: usize = 0;
const PAGE_SIZES: [usize; 6] = [PAGE_SIZE_ALL, 50, 100, 250, 500, 1000];
const COPY_SHORTCUT: KeyboardShortcut = KeyboardShortcut::new(Modifiers::COMMAND, Key::C);
const SAVE_SHORTCUT: KeyboardShortcut = KeyboardShortcut::new(Modifiers::COMMAND, Key::S);

#[derive(Clone, Debug, PartialEq, Eq)]
struct SelectedCell {
    row_key: i64,
    column_index: usize,
}

#[derive(Clone, Debug)]
struct EditingCell {
    row_key: i64,
    column_index: usize,
    draft_value: String,
    auto_focus: bool,
}

#[derive(Clone, Debug)]
enum ToastTone {
    Neutral,
    Success,
}

#[derive(Clone, Debug)]
struct Toast {
    message: String,
    created_at: Instant,
    tone: ToastTone,
}

pub struct ParquetApp {
    command_tx: Sender<WorkerCommand>,
    event_rx: Receiver<WorkerEvent>,
    next_request_id: u64,
    last_requested_id: u64,

    source_path: Option<PathBuf>,
    file_size_bytes: Option<u64>,
    schema: Vec<ColumnSchema>,
    rows: Vec<RowData>,
    total_rows: usize,
    filtered_rows: usize,

    page_index: usize,
    page_size: usize,
    sort: Option<SortSpec>,
    draft_filters: Vec<FilterRule>,
    applied_filters: Vec<FilterRule>,
    sql_query_input: String,
    applied_sql_query: Option<String>,
    next_filter_id: u64,

    is_edit_mode: bool,
    is_loading_file: bool,
    is_querying_rows: bool,
    is_preparing_edit: bool,
    is_saving: bool,

    selected_row_key: Option<i64>,
    selected_cell: Option<SelectedCell>,
    editing_cell: Option<EditingCell>,

    error_message: Option<String>,
    toast: Option<Toast>,
}

impl ParquetApp {
    pub fn new(startup_path: Option<PathBuf>) -> Self {
        let (command_tx, command_rx) = unbounded();
        let (event_tx, event_rx) = unbounded();
        spawn_worker(command_rx, event_tx);

        let mut app = Self {
            command_tx,
            event_rx,
            next_request_id: 1,
            last_requested_id: 0,
            source_path: None,
            file_size_bytes: None,
            schema: Vec::new(),
            rows: Vec::new(),
            total_rows: 0,
            filtered_rows: 0,
            page_index: 0,
            page_size: 100,
            sort: None,
            draft_filters: Vec::new(),
            applied_filters: Vec::new(),
            sql_query_input: String::new(),
            applied_sql_query: None,
            next_filter_id: 1,
            is_edit_mode: false,
            is_loading_file: false,
            is_querying_rows: false,
            is_preparing_edit: false,
            is_saving: false,
            selected_row_key: None,
            selected_cell: None,
            editing_cell: None,
            error_message: None,
            toast: None,
        };

        if let Some(path) = startup_path {
            app.open_path(path);
        }

        app
    }

    fn process_worker_events(&mut self) {
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                WorkerEvent::Snapshot {
                    request_id,
                    snapshot,
                    notice,
                } => {
                    if request_id < self.last_requested_id {
                        continue;
                    }

                    self.apply_snapshot(snapshot);
                    self.clear_busy_flags();
                    self.error_message = None;

                    if let Some(message) = notice {
                        let tone = if message.starts_with("Saved changes to ") {
                            ToastTone::Success
                        } else {
                            ToastTone::Neutral
                        };
                        self.show_toast(message, tone);
                    }
                }
                WorkerEvent::Error { request_id, message } => {
                    if request_id < self.last_requested_id {
                        continue;
                    }

                    self.clear_busy_flags();
                    self.error_message = Some(message);
                }
            }
        }
    }

    fn apply_snapshot(&mut self, snapshot: DatasetSnapshot) {
        self.source_path = Some(snapshot.source_path);
        self.file_size_bytes = Some(snapshot.file_size_bytes);
        self.schema = snapshot.schema;
        self.rows = snapshot.rows;
        self.total_rows = snapshot.total_rows;
        self.filtered_rows = snapshot.filtered_rows;
        self.is_edit_mode = snapshot.is_edit_mode;

        if let Some(selected_row_key) = self.selected_row_key {
            if !self.rows.iter().any(|row| row.row_key == selected_row_key) {
                self.selected_row_key = None;
            }
        }

        if let Some(selected_cell) = &self.selected_cell {
            let valid_row = self.rows.iter().any(|row| row.row_key == selected_cell.row_key);
            let valid_column = selected_cell.column_index < self.schema.len();
            if !valid_row || !valid_column {
                self.selected_cell = None;
            }
        }

        if let Some(editing_cell) = &self.editing_cell {
            let valid_row = self.rows.iter().any(|row| row.row_key == editing_cell.row_key);
            let valid_column = editing_cell.column_index < self.schema.len();
            if !valid_row || !valid_column || !self.is_edit_mode {
                self.editing_cell = None;
            }
        }

        let max_page_index = self.max_page_index();
        if self.page_index > max_page_index {
            self.page_index = max_page_index;
        }
    }

    fn render_header(&mut self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing = Vec2::new(10.0, 0.0);

            ui.heading(RichText::new("parquet++").size(26.0).strong());
            ui.add_space(12.0);

            let filename = self
                .source_path
                .as_ref()
                .and_then(|path| path.file_name())
                .and_then(|name| name.to_str())
                .unwrap_or("");

            ui.vertical(|ui| {
                if !filename.is_empty() {
                    ui.label(RichText::new(filename).size(18.0).strong());
                }

                ui.horizontal_wrapped(|ui| {
                    if let Some(file_size) = self.file_size_bytes {
                        self.badge(ui, format_bytes(file_size), ui.visuals().widgets.inactive.bg_fill, ui.visuals().strong_text_color());
                    }

                    if !self.schema.is_empty() {
                        self.badge(
                            ui,
                            format!("{} cols", self.schema.len()),
                            Color32::from_rgb(230, 240, 255),
                            Color32::from_rgb(26, 79, 144),
                        );
                    }

                    if self.total_rows > 0 {
                        self.badge(
                            ui,
                            format!("{} rows", self.total_rows),
                            Color32::from_rgb(236, 244, 239),
                            Color32::from_rgb(36, 97, 64),
                        );
                    }

                    if self.has_active_filters() {
                        self.badge(
                            ui,
                            format!("{} filter{}", self.applied_filters.len(), if self.applied_filters.len() == 1 { "" } else { "s" }),
                            Color32::from_rgb(255, 244, 228),
                            Color32::from_rgb(143, 82, 25),
                        );
                    }

                    if self.is_edit_mode {
                        self.badge(
                            ui,
                            "Edit mode".to_owned(),
                            Color32::from_rgb(255, 235, 232),
                            Color32::from_rgb(148, 58, 43),
                        );
                    }

                    if self.applied_sql_query.is_some() {
                        self.badge(
                            ui,
                            "SQL view".to_owned(),
                            Color32::from_rgb(240, 235, 255),
                            Color32::from_rgb(101, 58, 167),
                        );
                    }
                });
            });

            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                let busy = self.is_busy();

                if self.is_edit_mode {
                    if ui
                        .add_enabled(
                            !busy,
                            Button::new(RichText::new("Save As").strong()).min_size(Vec2::new(98.0, 34.0)),
                        )
                        .clicked()
                    {
                        self.save_as();
                    }

                    if ui
                        .add_enabled(
                            !busy && self.source_path.is_some(),
                            Button::new(RichText::new("Save").strong())
                                .fill(Color32::from_rgb(31, 122, 82))
                                .min_size(Vec2::new(90.0, 34.0)),
                        )
                        .clicked()
                    {
                        self.save_to_existing_path();
                    }

                    if ui
                        .add_enabled(!busy, Button::new("Cancel").min_size(Vec2::new(92.0, 34.0)))
                        .clicked()
                    {
                        self.cancel_edit_mode();
                    }
                }

                if ui
                    .add_enabled(
                        !busy,
                        Button::new(RichText::new("Open File").strong()).min_size(Vec2::new(106.0, 34.0)),
                    )
                    .clicked()
                {
                    self.pick_and_open_file();
                }
            });
        });
    }

    fn render_toolbar(&mut self, ui: &mut Ui) {
        ui.horizontal_wrapped(|ui| {
            ui.spacing_mut().item_spacing = Vec2::new(8.0, 8.0);

            if self.has_data() && !self.is_edit_mode && self.applied_sql_query.is_none() {
                if ui
                    .add_enabled(
                        !self.is_busy(),
                        Button::new(RichText::new("Edit").strong())
                            .fill(Color32::from_rgb(202, 74, 31))
                            .min_size(Vec2::new(90.0, 34.0)),
                    )
                    .clicked()
                {
                    self.begin_edit_mode();
                }
            }

            if ui
                .add_enabled(
                    self.has_data() && !self.is_saving && self.applied_sql_query.is_none(),
                    Button::new("Add Filter").min_size(Vec2::new(100.0, 34.0)),
                )
                .clicked()
            {
                self.add_filter();
            }

            if ui
                .add_enabled(
                    (!self.draft_filters.is_empty() || !self.applied_filters.is_empty()) && !self.is_busy(),
                    Button::new("Clear").min_size(Vec2::new(86.0, 34.0)),
                )
                .clicked()
            {
                self.clear_filters();
            }

            if self.has_pending_filter_changes() {
                self.badge(
                    ui,
                    "Filters changed. Press Enter in a filter field.".to_owned(),
                    Color32::from_rgb(255, 242, 223),
                    Color32::from_rgb(145, 86, 23),
                );
            }

            ui.separator();
            ui.label("Rows per page");

            ComboBox::from_id_salt("page-size")
                .selected_text(page_size_label(self.page_size))
                .show_ui(ui, |ui| {
                    for option in PAGE_SIZES {
                        if ui
                            .selectable_label(self.page_size == option, page_size_label(option))
                            .clicked()
                        {
                            self.page_size = option;
                            self.page_index = 0;
                            self.refresh_rows();
                        }
                    }
                });

            ui.separator();

            let page_text = if self.page_size == PAGE_SIZE_ALL {
                "All rows".to_owned()
            } else {
                format!("Page {} of {}", self.page_index + 1, self.max_page_index() + 1)
            };
            ui.label(page_text);

            if ui
                .add_enabled(
                    self.page_size != PAGE_SIZE_ALL && self.page_index > 0 && !self.is_busy(),
                    Button::new("First").min_size(Vec2::new(76.0, 34.0)),
                )
                .clicked()
            {
                self.page_index = 0;
                self.refresh_rows();
            }

            if ui
                .add_enabled(
                    self.page_size != PAGE_SIZE_ALL && self.page_index > 0 && !self.is_busy(),
                    Button::new("Previous").min_size(Vec2::new(96.0, 34.0)),
                )
                .clicked()
            {
                self.page_index -= 1;
                self.refresh_rows();
            }

            if ui
                .add_enabled(
                    self.page_size != PAGE_SIZE_ALL && self.page_index < self.max_page_index() && !self.is_busy(),
                    Button::new("Next").min_size(Vec2::new(78.0, 34.0)),
                )
                .clicked()
            {
                self.page_index += 1;
                self.refresh_rows();
            }

            if ui
                .add_enabled(
                    self.page_size != PAGE_SIZE_ALL && self.page_index < self.max_page_index() && !self.is_busy(),
                    Button::new("Last").min_size(Vec2::new(74.0, 34.0)),
                )
                .clicked()
            {
                self.page_index = self.max_page_index();
                self.refresh_rows();
            }
        });

        if self.has_data() {
            ui.add_space(8.0);
            Frame::new()
                .fill(Color32::from_rgb(250, 251, 254))
                .corner_radius(CornerRadius::same(10))
                .stroke(Stroke::new(1.0, Color32::from_rgb(228, 232, 239)))
                .inner_margin(egui::Margin::symmetric(12, 10))
                .show(ui, |ui| {
                    ui.horizontal_wrapped(|ui| {
                        ui.label(RichText::new("SQL").strong().color(Color32::from_rgb(54, 66, 82)));

                        let sql_enabled = !self.is_busy() && !self.is_edit_mode;
                        let response = ui.add_enabled(
                            sql_enabled,
                            TextEdit::singleline(&mut self.sql_query_input)
                                .desired_width(560.0)
                                .hint_text("Use SELECT ... FROM data ... then press Enter"),
                        );

                        if response.lost_focus() && ui.input(|input| input.key_pressed(Key::Enter)) && sql_enabled {
                            self.apply_sql_query();
                        }

                        if ui
                            .add_enabled(
                                sql_enabled && self.applied_sql_query.is_some(),
                                Button::new("Clear SQL").min_size(Vec2::new(96.0, 34.0)),
                            )
                            .clicked()
                        {
                            self.clear_sql_query();
                        }

                        ui.label(
                            RichText::new("Syntax: SELECT * FROM data WHERE country = 'US'")
                                .size(12.5)
                                .color(Color32::from_rgb(111, 122, 139)),
                        );
                    });

                    if self.is_edit_mode {
                        ui.add_space(6.0);
                        ui.label(
                            RichText::new("Exit edit mode before running a SQL query.")
                                .size(12.5)
                                .color(Color32::from_rgb(145, 86, 23)),
                        );
                    }
                });
        }

        if let Some(toast) = &self.toast {
            if toast.created_at.elapsed() < Duration::from_secs(4) && matches!(toast.tone, ToastTone::Success) {
                ui.add_space(8.0);
                Frame::new()
                    .fill(Color32::from_rgb(235, 247, 239))
                    .corner_radius(CornerRadius::same(10))
                    .stroke(Stroke::new(1.0, Color32::from_rgb(169, 216, 184)))
                    .inner_margin(egui::Margin::symmetric(12, 10))
                    .show(ui, |ui| {
                        ui.horizontal_wrapped(|ui| {
                            ui.label(
                                RichText::new("Save complete")
                                    .strong()
                                    .color(Color32::from_rgb(30, 98, 63)),
                            );
                            ui.label(
                                RichText::new(&toast.message)
                                    .color(Color32::from_rgb(45, 107, 74)),
                            );
                        });
                    });
            }
        }

        if !self.draft_filters.is_empty() {
            ui.add_space(8.0);

            let schema_names = self
                .schema
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();

            let schema_lookup = self.schema.clone();

            let mut remove_id = None;
            let mut should_apply_filters = false;

            for filter in &mut self.draft_filters {
                Frame::new()
                    .fill(Color32::from_rgb(249, 250, 252))
                    .corner_radius(CornerRadius::same(10))
                    .stroke(Stroke::new(1.0, Color32::from_rgb(225, 229, 236)))
                    .inner_margin(egui::Margin::symmetric(10, 8))
                    .show(ui, |ui| {
                        ui.horizontal_wrapped(|ui| {
                            ComboBox::from_id_salt(format!("filter-column-{}", filter.id))
                                .selected_text(filter.column_name.clone())
                                .show_ui(ui, |ui| {
                                    for name in &schema_names {
                                        ui.selectable_value(&mut filter.column_name, name.clone(), name);
                                    }
                                });

                            let category = schema_lookup
                                .iter()
                                .find(|column| column.name == filter.column_name)
                                .map(|column| &column.category)
                                .unwrap_or(&ColumnCategory::Text);

                            ComboBox::from_id_salt(format!("filter-operator-{}", filter.id))
                                .selected_text(operator_label(filter.operator))
                                .show_ui(ui, |ui| {
                                    for operator in operator_options(category) {
                                        ui.selectable_value(&mut filter.operator, operator, operator_label(operator));
                                    }
                                });

                            if filter.operator.needs_value() {
                                let response = ui.add_sized(
                                    [200.0, 28.0],
                                    TextEdit::singleline(&mut filter.value).hint_text("Value"),
                                );

                                if response.lost_focus() && ui.input(|input| input.key_pressed(Key::Enter)) {
                                    should_apply_filters = true;
                                }
                            }

                            if ui
                                .add(Button::new("Remove").min_size(Vec2::new(84.0, 30.0)))
                                .clicked()
                            {
                                remove_id = Some(filter.id);
                                should_apply_filters = true;
                            }
                        });
                    });

                ui.add_space(6.0);
            }

            if let Some(filter_id) = remove_id {
                self.draft_filters.retain(|filter| filter.id != filter_id);
            }

            if should_apply_filters && self.has_pending_filter_changes() && !self.is_busy() {
                self.apply_filters();
            }
        }
    }

    fn render_table(&mut self, ui: &mut Ui, ctx: &Context) {
        if !self.has_data() {
            self.render_empty_state(ui);
            return;
        }

        let row_height = 34.0;
        let schema = self.schema.clone();
        let rows = self.rows.clone();
        Frame::new()
            .fill(Color32::from_rgb(255, 255, 255))
            .corner_radius(CornerRadius::same(14))
            .stroke(Stroke::new(1.0, Color32::from_rgb(221, 226, 234)))
            .inner_margin(egui::Margin::same(8))
            .show(ui, |ui| {
                let mut table = TableBuilder::new(ui)
                    .striped(false)
                    .resizable(true)
                    .cell_layout(Layout::left_to_right(Align::Center))
                    .min_scrolled_height(200.0);

                for (column_index, column) in schema.iter().enumerate() {
                    table = table.column(Column::initial(self.column_width_hint(column_index, column)).at_least(78.0));
                }

                table
                    .header(42.0, |mut header| {
                        for (column_index, column) in schema.iter().enumerate() {
                            header.col(|ui| {
                                self.render_header_cell(ui, column_index, column);
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(row_height, rows.len(), |mut row| {
                            let row_index = row.index();
                            let row_data = &rows[row_index];
                            let row_selected = self.selected_row_key == Some(row_data.row_key);

                            for (column_index, cell) in row_data.cells.iter().enumerate() {
                                row.col(|ui| {
                                    let is_cell_selected = self.selected_cell.as_ref()
                                        == Some(&SelectedCell {
                                            row_key: row_data.row_key,
                                            column_index,
                                        });
                                    let is_cell_editing = self
                                        .editing_cell
                                        .as_ref()
                                        .map(|editing| {
                                            editing.row_key == row_data.row_key && editing.column_index == column_index
                                        })
                                        .unwrap_or(false);

                                    self.render_data_cell(
                                        ui,
                                        ctx,
                                        row_data.row_key,
                                        row_index,
                                        column_index,
                                        cell,
                                        row_selected,
                                        is_cell_selected,
                                        is_cell_editing,
                                    );
                                });
                            }
                        });
                    });
            });
    }

    fn render_header_cell(&mut self, ui: &mut Ui, column_index: usize, column: &ColumnSchema) {
        let is_sorted = self
            .sort
            .as_ref()
            .map(|sort| sort.column_name == column.name)
            .unwrap_or(false);

        let direction_marker = if is_sorted {
            match self.sort.as_ref().map(|sort| sort.direction) {
                Some(SortDirection::Asc) => " ↑",
                Some(SortDirection::Desc) => " ↓",
                None => "",
            }
        } else {
            ""
        };

        let button_fill = if is_sorted {
            Color32::from_rgb(230, 239, 255)
        } else {
            Color32::from_rgb(245, 247, 250)
        };
        let response = ui.add_sized(
            [ui.available_width(), 42.0],
            Button::new("")
                .fill(button_fill)
                .stroke(Stroke::new(1.0, Color32::from_rgb(219, 224, 232))),
        );

        response
            .clone()
            .on_hover_text(format!("{} ({})", column.name, column.declared_type));

        let icon = if is_sorted {
            direction_marker.trim().to_owned()
        } else {
            "↕".to_owned()
        };

        let rect = response.rect.shrink2(Vec2::new(10.0, 5.0));
        let painter = ui.painter();
        painter.text(
            rect.left_top(),
            egui::Align2::LEFT_TOP,
            &column.name,
            egui::FontId::proportional(14.0),
            Color32::from_rgb(26, 32, 44),
        );
        painter.text(
            rect.left_bottom(),
            egui::Align2::LEFT_BOTTOM,
            &column.declared_type,
            egui::FontId::proportional(11.5),
            Color32::from_rgb(112, 122, 138),
        );
        painter.text(
            rect.right_center(),
            egui::Align2::RIGHT_CENTER,
            icon,
            egui::FontId::proportional(15.0),
            if is_sorted {
                Color32::from_rgb(46, 93, 194)
            } else {
                Color32::from_rgb(122, 132, 148)
            },
        );

        if response.clicked() && !self.is_busy() {
            self.cycle_sort(column_index);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn render_data_cell(
        &mut self,
        ui: &mut Ui,
        ctx: &Context,
        row_key: i64,
        row_index: usize,
        column_index: usize,
        cell: &CellValue,
        row_selected: bool,
        is_cell_selected: bool,
        is_cell_editing: bool,
    ) {
        let fill = if is_cell_selected {
            Color32::from_rgb(218, 232, 255)
        } else if row_selected {
            Color32::from_rgb(236, 244, 255)
        } else if row_index.is_multiple_of(2) {
            Color32::from_rgb(255, 255, 255)
        } else {
            Color32::from_rgb(249, 251, 253)
        };

        Frame::new()
            .fill(fill)
            .stroke(if is_cell_selected {
                Stroke::new(1.0, Color32::from_rgb(67, 112, 220))
            } else {
                Stroke::new(1.0, Color32::from_rgb(234, 238, 243))
            })
            .corner_radius(CornerRadius::same(4))
            .inner_margin(egui::Margin::symmetric(10, 5))
            .show(ui, |ui| {
                ui.set_min_height(26.0);

                if is_cell_editing {
                    if let Some(editing_cell) = self.editing_cell.as_mut() {
                        let response = ui.add_sized(
                            [ui.available_width(), 26.0],
                            TextEdit::singleline(&mut editing_cell.draft_value)
                                .text_color(Color32::from_rgb(20, 27, 38))
                                .desired_width(f32::INFINITY),
                        );

                        if editing_cell.auto_focus {
                            response.request_focus();
                            editing_cell.auto_focus = false;
                        }

                        let enter_pressed = ui.input(|input| input.key_pressed(Key::Enter));
                        let escape_pressed = ui.input(|input| input.key_pressed(Key::Escape));

                        if escape_pressed {
                            self.editing_cell = None;
                        } else if enter_pressed || response.lost_focus() {
                            self.commit_editing_cell();
                        }
                    }
                    return;
                }

                let text = cell_text(cell);
                let rich_text = match cell {
                    CellValue::Null => RichText::new(text)
                        .italics()
                        .size(14.5)
                        .color(Color32::from_rgb(116, 125, 140)),
                    CellValue::Bool(true) => RichText::new(text)
                        .strong()
                        .size(14.5)
                        .color(Color32::from_rgb(27, 107, 72)),
                    CellValue::Bool(false) => RichText::new(text)
                        .strong()
                        .size(14.5)
                        .color(Color32::from_rgb(167, 59, 47)),
                    CellValue::Text(_) => RichText::new(text)
                        .size(14.5)
                        .color(Color32::from_rgb(20, 27, 38)),
                };

                let response = ui.add(
                    egui::Label::new(rich_text)
                        .sense(Sense::click())
                        .truncate(),
                );

                response.clone().on_hover_text(cell.as_copy_text());

                if response.clicked() {
                    self.selected_row_key = Some(row_key);
                    self.selected_cell = Some(SelectedCell {
                        row_key,
                        column_index,
                    });

                    if self.is_edit_mode && !self.is_busy() {
                        self.editing_cell = Some(EditingCell {
                            row_key,
                            column_index,
                            draft_value: cell.as_copy_text(),
                            auto_focus: true,
                        });
                        ctx.request_repaint();
                    }
                }
            });
    }

    fn render_empty_state(&mut self, ui: &mut Ui) {
        ui.vertical_centered(|ui| {
            ui.add_space(120.0);
            ui.label(RichText::new("Open a parquet file to start browsing rows").size(22.0).strong());
            ui.add_space(8.0);
            ui.label(
                RichText::new(
                    "Sorting, filtering, row selection, cell selection, inline editing, and parquet save are all available once a file is open.",
                )
                .size(15.0)
                .color(Color32::from_rgb(110, 118, 132)),
            );
            ui.add_space(18.0);

            if ui
                .add(Button::new(RichText::new("Open File").strong()).min_size(Vec2::new(140.0, 40.0)))
                .clicked()
            {
                self.pick_and_open_file();
            }
        });
    }

    fn render_status_bar(&mut self, ui: &mut Ui) {
        ui.horizontal_wrapped(|ui| {
            if self.has_data() {
                let start = if self.rows.is_empty() {
                    0
                } else {
                    self.page_index.saturating_mul(self.page_size) + 1
                };
                let end = self.page_index.saturating_mul(self.page_size) + self.rows.len();
                ui.label(format!("Showing {start}-{end} of {} rows", self.filtered_rows));

                if self.filtered_rows != self.total_rows {
                    ui.label(format!("({} total in file)", self.total_rows));
                }

                if self.is_busy() {
                    ui.label("Working...");
                }
            } else {
                ui.label("No parquet file loaded");
            }

            if let Some(error_message) = &self.error_message {
                ui.colored_label(Color32::from_rgb(166, 58, 47), error_message);
            } else if let Some(toast) = &self.toast {
                if toast.created_at.elapsed() < Duration::from_secs(4) {
                    let color = match toast.tone {
                        ToastTone::Neutral => Color32::from_rgb(38, 110, 73),
                        ToastTone::Success => Color32::from_rgb(27, 107, 72),
                    };
                    ui.colored_label(color, &toast.message);
                }
            }
        });
    }

    fn process_shortcuts(&mut self, ctx: &Context) {
        if ctx.input_mut(|input| input.consume_shortcut(&COPY_SHORTCUT)) {
            if let Some(value) = self.selected_cell_value() {
                ctx.copy_text(value.clone());
                self.show_toast("Cell copied".to_owned(), ToastTone::Neutral);
            }
        }

        if ctx.input_mut(|input| input.consume_shortcut(&SAVE_SHORTCUT)) && self.is_edit_mode && !self.is_busy() {
            self.save_to_existing_path();
        }
    }

    fn has_data(&self) -> bool {
        !self.schema.is_empty()
    }

    fn has_active_filters(&self) -> bool {
        !self.applied_filters.is_empty()
    }

    fn is_busy(&self) -> bool {
        self.is_loading_file || self.is_querying_rows || self.is_preparing_edit || self.is_saving
    }

    fn show_toast(&mut self, message: String, tone: ToastTone) {
        self.toast = Some(Toast {
            message,
            created_at: Instant::now(),
            tone,
        });
    }

    fn clear_busy_flags(&mut self) {
        self.is_loading_file = false;
        self.is_querying_rows = false;
        self.is_preparing_edit = false;
        self.is_saving = false;
    }

    fn dispatch(&mut self, build_command: impl FnOnce(u64) -> WorkerCommand) {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        self.last_requested_id = request_id;
        let _ = self.command_tx.send(build_command(request_id));
    }

    fn query_spec(&self) -> QuerySpec {
        QuerySpec {
            page_index: self.page_index,
            page_size: self.page_size,
            sort: self.sort.clone(),
            filters: self.applied_filters.clone(),
            sql_query: self.applied_sql_query.clone(),
        }
    }

    fn pick_and_open_file(&mut self) {
        if let Some(path) = FileDialog::new()
            .add_filter("Parquet files", &["parquet"])
            .pick_file()
        {
            self.open_path(path);
        }
    }

    fn open_path(&mut self, path: PathBuf) {
        self.is_loading_file = true;
        self.error_message = None;
        self.page_index = 0;
        self.sort = None;
        self.selected_row_key = None;
        self.selected_cell = None;
        self.editing_cell = None;
        self.draft_filters.clear();
        self.applied_filters.clear();
        self.sql_query_input.clear();
        self.applied_sql_query = None;
        let page_size = self.page_size;

        self.dispatch(|request_id| WorkerCommand::LoadFile {
            request_id,
            path,
            query: QuerySpec {
                page_index: 0,
                page_size,
                sort: None,
                filters: Vec::new(),
                sql_query: None,
            },
        });
    }

    fn refresh_rows(&mut self) {
        if !self.has_data() {
            return;
        }

        self.is_querying_rows = true;
        let query = self.query_spec();
        self.dispatch(|request_id| WorkerCommand::Refresh { request_id, query });
    }

    fn begin_edit_mode(&mut self) {
        if !self.has_data() {
            return;
        }

        self.is_preparing_edit = true;
        let query = self.query_spec();
        self.dispatch(|request_id| WorkerCommand::BeginEdit { request_id, query });
    }

    fn cancel_edit_mode(&mut self) {
        if !self.is_edit_mode {
            return;
        }

        self.is_preparing_edit = true;
        self.editing_cell = None;
        let query = self.query_spec();
        self.dispatch(|request_id| WorkerCommand::CancelEdit { request_id, query });
    }

    fn save_to_existing_path(&mut self) {
        if !self.is_edit_mode || self.is_busy() {
            return;
        }

        if let Some(path) = self.source_path.clone() {
            self.is_saving = true;
            let query = self.query_spec();
            self.dispatch(|request_id| WorkerCommand::Save {
                request_id,
                destination: path,
                query,
            });
        } else {
            self.save_as();
        }
    }

    fn save_as(&mut self) {
        if !self.is_edit_mode || self.is_busy() {
            return;
        }

        let suggested = self
            .source_path
            .as_ref()
            .map(|path| edited_file_name(path))
            .unwrap_or_else(|| "edited.parquet".to_owned());

        if let Some(path) = FileDialog::new()
            .add_filter("Parquet files", &["parquet"])
            .set_file_name(&suggested)
            .save_file()
        {
            self.is_saving = true;
            let query = self.query_spec();
            self.dispatch(|request_id| WorkerCommand::Save {
                request_id,
                destination: path,
                query,
            });
        }
    }

    fn add_filter(&mut self) {
        if self.schema.is_empty() {
            return;
        }

        let column_name = self.schema[0].name.clone();
        self.draft_filters.push(FilterRule {
            id: self.next_filter_id,
            column_name,
            operator: FilterOperator::Contains,
            value: String::new(),
        });
        self.next_filter_id += 1;
    }

    fn apply_filters(&mut self) {
        self.applied_filters = self.draft_filters.clone();
        self.page_index = 0;
        self.refresh_rows();
    }

    fn clear_filters(&mut self) {
        self.draft_filters.clear();
        self.applied_filters.clear();
        self.page_index = 0;
        self.refresh_rows();
    }

    fn has_pending_filter_changes(&self) -> bool {
        self.draft_filters != self.applied_filters
    }

    fn apply_sql_query(&mut self) {
        if !self.has_data() || self.is_busy() || self.is_edit_mode {
            return;
        }

        let trimmed = self.sql_query_input.trim();
        self.applied_sql_query = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        };
        self.page_index = 0;
        self.sort = None;
        self.selected_row_key = None;
        self.selected_cell = None;
        self.editing_cell = None;
        self.refresh_rows();
    }

    fn clear_sql_query(&mut self) {
        self.sql_query_input.clear();
        self.applied_sql_query = None;
        self.page_index = 0;
        self.sort = None;
        self.selected_row_key = None;
        self.selected_cell = None;
        self.editing_cell = None;
        self.refresh_rows();
    }

    fn cycle_sort(&mut self, column_index: usize) {
        let column_name = self.schema[column_index].name.clone();

        self.sort = match self.sort.as_ref() {
            Some(sort) if sort.column_name == column_name && sort.direction == SortDirection::Asc => Some(SortSpec {
                column_name,
                direction: SortDirection::Desc,
            }),
            Some(sort) if sort.column_name == column_name && sort.direction == SortDirection::Desc => None,
            _ => Some(SortSpec {
                column_name,
                direction: SortDirection::Asc,
            }),
        };

        self.page_index = 0;
        self.refresh_rows();
    }

    fn max_page_index(&self) -> usize {
        if self.page_size == PAGE_SIZE_ALL || self.filtered_rows == 0 {
            0
        } else {
            (self.filtered_rows - 1) / self.page_size.max(1)
        }
    }

    fn selected_cell_value(&self) -> Option<String> {
        let selected = self.selected_cell.as_ref()?;
        let row = self.rows.iter().find(|row| row.row_key == selected.row_key)?;
        let cell = row.cells.get(selected.column_index)?;
        Some(cell.as_copy_text())
    }

    fn column_width_hint(&self, column_index: usize, column: &ColumnSchema) -> f32 {
        let header_chars = column.name.chars().count().max(column.declared_type.chars().count());
        let sample_chars = self
            .rows
            .iter()
            .take(50)
            .filter_map(|row| row.cells.get(column_index))
            .map(|cell| cell.as_copy_text().chars().take(36).count())
            .max()
            .unwrap_or(0);
        let max_chars = header_chars.max(sample_chars);
        (max_chars as f32 * 8.2 + 28.0).clamp(84.0, 320.0)
    }

    fn commit_editing_cell(&mut self) {
        let Some(editing_cell) = self.editing_cell.take() else {
            return;
        };

        let Some(column) = self.schema.get(editing_cell.column_index) else {
            return;
        };

        let set_null = editing_cell.draft_value.trim().eq_ignore_ascii_case("null");
        self.is_querying_rows = true;
        let query = self.query_spec();
        let column_name = column.name.clone();

        self.dispatch(|request_id| WorkerCommand::UpdateCell {
            request_id,
            row_key: editing_cell.row_key,
            column_name: column_name.clone(),
            new_value: editing_cell.draft_value,
            set_null,
            query,
        });
    }

    fn apply_theme(&self, ctx: &Context) {
        let mut style = (*ctx.global_style()).clone();
        style.spacing.item_spacing = Vec2::new(10.0, 10.0);
        style.spacing.button_padding = Vec2::new(12.0, 8.0);
        style.spacing.interact_size = Vec2::new(40.0, 34.0);
        style.visuals.widgets.inactive.bg_fill = Color32::from_rgb(248, 249, 252);
        style.visuals.widgets.hovered.bg_fill = Color32::from_rgb(239, 242, 248);
        style.visuals.widgets.active.bg_fill = Color32::from_rgb(223, 235, 255);
        style.visuals.window_fill = Color32::from_rgb(252, 252, 253);
        style.visuals.panel_fill = Color32::from_rgb(247, 249, 252);
        style.visuals.extreme_bg_color = Color32::from_rgb(244, 247, 251);
        style.visuals.selection.bg_fill = Color32::from_rgb(223, 235, 255);
        style.visuals.selection.stroke = Stroke::new(1.0, Color32::from_rgb(74, 121, 235));
        ctx.set_global_style(style);
    }

    fn badge(&self, ui: &mut Ui, text: String, fill: Color32, text_color: Color32) {
        Frame::new()
            .fill(fill)
            .corner_radius(CornerRadius::same(255))
            .inner_margin(egui::Margin::symmetric(8, 4))
            .show(ui, |ui| {
                ui.label(RichText::new(text).color(text_color).size(12.5).strong());
            });
    }
}

impl eframe::App for ParquetApp {
    fn ui(&mut self, _ui: &mut Ui, _frame: &mut eframe::Frame) {}

    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        self.apply_theme(ctx);
        self.process_worker_events();
        self.process_shortcuts(ctx);
        self.process_dropped_files(ctx);

        if self.is_busy() {
            ctx.request_repaint_after(Duration::from_millis(32));
        }

        egui::TopBottomPanel::top("top-panel")
            .frame(
                Frame::new()
                    .fill(Color32::from_rgb(252, 252, 253))
                    .inner_margin(egui::Margin::same(16))
                    .stroke(Stroke::new(1.0, Color32::from_rgb(230, 233, 238))),
            )
            .show(ctx, |ui| {
                self.render_header(ui);
                ui.add_space(12.0);
                self.render_toolbar(ui);
            });

        egui::TopBottomPanel::bottom("status-panel")
            .frame(
                Frame::new()
                    .fill(Color32::from_rgb(248, 249, 251))
                    .inner_margin(egui::Margin::same(12))
                    .stroke(Stroke::new(1.0, Color32::from_rgb(230, 233, 238))),
            )
            .show(ctx, |ui| {
                self.render_status_bar(ui);
            });

        egui::CentralPanel::default()
            .frame(
                Frame::new()
                    .fill(Color32::from_rgb(247, 249, 252))
                    .inner_margin(egui::Margin::same(12)),
            )
            .show(ctx, |ui| {
                ScrollArea::both()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        self.render_table(ui, ctx);
                    });
            });
    }

    fn on_exit(&mut self) {
        let _ = self.command_tx.send(WorkerCommand::Shutdown);
    }
}

impl ParquetApp {
    fn process_dropped_files(&mut self, ctx: &Context) {
        let dropped_paths = ctx.input(|input| {
            input
                .raw
                .dropped_files
                .iter()
                .filter_map(|file| file.path.clone())
                .collect::<Vec<_>>()
        });

        if let Some(path) = dropped_paths.into_iter().next() {
            self.open_path(path);
        }
    }
}

fn cell_text(cell: &CellValue) -> String {
    match cell {
        CellValue::Null => "null".to_owned(),
        CellValue::Bool(value) => value.to_string(),
        CellValue::Text(value) => value.clone(),
    }
}

fn operator_options(category: &ColumnCategory) -> Vec<FilterOperator> {
    match category {
        ColumnCategory::Boolean => vec![
            FilterOperator::Equals,
            FilterOperator::NotEquals,
            FilterOperator::IsNull,
            FilterOperator::IsNotNull,
        ],
        ColumnCategory::Number | ColumnCategory::Temporal => vec![
            FilterOperator::Equals,
            FilterOperator::NotEquals,
            FilterOperator::GreaterThan,
            FilterOperator::GreaterThanOrEqual,
            FilterOperator::LessThan,
            FilterOperator::LessThanOrEqual,
            FilterOperator::IsNull,
            FilterOperator::IsNotNull,
        ],
        ColumnCategory::Text | ColumnCategory::Other => vec![
            FilterOperator::Contains,
            FilterOperator::StartsWith,
            FilterOperator::Equals,
            FilterOperator::NotEquals,
            FilterOperator::IsNull,
            FilterOperator::IsNotNull,
        ],
    }
}

fn operator_label(operator: FilterOperator) -> &'static str {
    match operator {
        FilterOperator::Contains => "contains",
        FilterOperator::Equals => "equals",
        FilterOperator::NotEquals => "not equals",
        FilterOperator::GreaterThan => ">",
        FilterOperator::GreaterThanOrEqual => ">=",
        FilterOperator::LessThan => "<",
        FilterOperator::LessThanOrEqual => "<=",
        FilterOperator::StartsWith => "starts with",
        FilterOperator::IsNull => "is null",
        FilterOperator::IsNotNull => "is not null",
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];

    let mut value = bytes as f64;
    let mut unit_index = 0usize;

    while value >= 1024.0 && unit_index < UNITS.len() - 1 {
        value /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{value:.0} {}", UNITS[unit_index])
    } else {
        format!("{value:.1} {}", UNITS[unit_index])
    }
}

fn edited_file_name(path: &Path) -> String {
    let file_name = path.file_name().and_then(|name| name.to_str()).unwrap_or("data.parquet");
    if let Some(base) = file_name.strip_suffix(".parquet") {
        format!("{base}-edited.parquet")
    } else {
        format!("{file_name}-edited.parquet")
    }
}

fn page_size_label(page_size: usize) -> String {
    if page_size == PAGE_SIZE_ALL {
        "All".to_owned()
    } else {
        page_size.to_string()
    }
}
