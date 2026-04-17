use std::{
    fs,
    path::{Path, PathBuf},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, NaiveTime, Utc};
use crossbeam_channel::{Receiver, Sender};
use duckdb::{Connection, Statement, params};
use duckdb::types::{TimeUnit, ValueRef};

const SOURCE_VIEW: &str = "parquet_source";
const EDIT_TABLE: &str = "editable_data";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnCategory {
    Text,
    Number,
    Boolean,
    Temporal,
    Other,
}

#[derive(Clone, Debug)]
pub struct ColumnSchema {
    pub name: String,
    pub declared_type: String,
    pub category: ColumnCategory,
}

#[derive(Clone, Debug)]
pub enum CellValue {
    Null,
    Bool(bool),
    Text(String),
}

impl CellValue {
    pub fn as_copy_text(&self) -> String {
        match self {
            Self::Null => "null".to_owned(),
            Self::Bool(value) => value.to_string(),
            Self::Text(value) => value.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RowData {
    pub row_key: i64,
    pub cells: Vec<CellValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortSpec {
    pub column_name: String,
    pub direction: SortDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterOperator {
    Contains,
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    StartsWith,
    IsNull,
    IsNotNull,
}

impl FilterOperator {
    pub fn needs_value(self) -> bool {
        !matches!(self, Self::IsNull | Self::IsNotNull)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterRule {
    pub id: u64,
    pub column_name: String,
    pub operator: FilterOperator,
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct QuerySpec {
    pub page_index: usize,
    pub page_size: usize,
    pub sort: Option<SortSpec>,
    pub filters: Vec<FilterRule>,
    pub sql_query: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DatasetSnapshot {
    pub source_path: PathBuf,
    pub file_size_bytes: u64,
    pub schema: Vec<ColumnSchema>,
    pub rows: Vec<RowData>,
    pub total_rows: usize,
    pub filtered_rows: usize,
    pub is_edit_mode: bool,
}

#[derive(Debug)]
pub enum WorkerCommand {
    LoadFile {
        request_id: u64,
        path: PathBuf,
        query: QuerySpec,
    },
    Refresh {
        request_id: u64,
        query: QuerySpec,
    },
    BeginEdit {
        request_id: u64,
        query: QuerySpec,
    },
    CancelEdit {
        request_id: u64,
        query: QuerySpec,
    },
    UpdateCell {
        request_id: u64,
        row_key: i64,
        column_name: String,
        new_value: String,
        set_null: bool,
        query: QuerySpec,
    },
    Save {
        request_id: u64,
        destination: PathBuf,
        query: QuerySpec,
    },
    Shutdown,
}

#[derive(Debug)]
pub enum WorkerEvent {
    Snapshot {
        request_id: u64,
        snapshot: DatasetSnapshot,
        notice: Option<String>,
    },
    Error {
        request_id: u64,
        message: String,
    },
}

pub fn spawn_worker(command_rx: Receiver<WorkerCommand>, event_tx: Sender<WorkerEvent>) {
    thread::spawn(move || {
        let mut worker = DbWorker::default();

        while let Ok(command) = command_rx.recv() {
            if matches!(command, WorkerCommand::Shutdown) {
                break;
            }

            let request_id = command.request_id();
            let result = worker.handle(command);

            match result {
                Ok((snapshot, notice)) => {
                    let _ = event_tx.send(WorkerEvent::Snapshot {
                        request_id,
                        snapshot,
                        notice,
                    });
                }
                Err(error) => {
                    let _ = event_tx.send(WorkerEvent::Error {
                        request_id,
                        message: format!("{error:#}"),
                    });
                }
            }
        }
    });
}

impl WorkerCommand {
    fn request_id(&self) -> u64 {
        match self {
            Self::LoadFile { request_id, .. }
            | Self::Refresh { request_id, .. }
            | Self::BeginEdit { request_id, .. }
            | Self::CancelEdit { request_id, .. }
            | Self::UpdateCell { request_id, .. }
            | Self::Save { request_id, .. } => *request_id,
            Self::Shutdown => 0,
        }
    }
}

#[derive(Default)]
struct DbWorker {
    session: Option<DatasetSession>,
}

impl DbWorker {
    fn handle(&mut self, command: WorkerCommand) -> Result<(DatasetSnapshot, Option<String>)> {
        match command {
            WorkerCommand::LoadFile { path, query, .. } => {
                let session = DatasetSession::load(path)?;
                let snapshot = session.snapshot(&query)?;
                self.session = Some(session);
                Ok((snapshot, None))
            }
            WorkerCommand::Refresh { query, .. } => {
                let session = self.session.as_mut().context("No parquet file is currently open.")?;
                let snapshot = session.snapshot(&query)?;
                Ok((snapshot, None))
            }
            WorkerCommand::BeginEdit { query, .. } => {
                let session = self.session.as_mut().context("No parquet file is currently open.")?;
                session.begin_edit()?;
                let snapshot = session.snapshot(&query)?;
                Ok((snapshot, None))
            }
            WorkerCommand::CancelEdit { query, .. } => {
                let session = self.session.as_mut().context("No parquet file is currently open.")?;
                session.cancel_edit()?;
                let snapshot = session.snapshot(&query)?;
                Ok((snapshot, None))
            }
            WorkerCommand::UpdateCell {
                row_key,
                column_name,
                new_value,
                set_null,
                query,
                ..
            } => {
                let session = self.session.as_mut().context("No parquet file is currently open.")?;
                session.update_cell(row_key, &column_name, &new_value, set_null)?;
                let snapshot = session.snapshot(&query)?;
                Ok((snapshot, Some("Cell updated".to_owned())))
            }
            WorkerCommand::Save {
                destination, query, ..
            } => {
                let session = self.session.as_mut().context("No parquet file is currently open.")?;
                session.save_to(&destination)?;
                let snapshot = session.snapshot(&query)?;
                let saved_name = destination
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("parquet file");
                let notice = format!("Saved changes to {saved_name}");
                Ok((snapshot, Some(notice)))
            }
            WorkerCommand::Shutdown => bail!("Shutdown should be handled before calling DbWorker::handle"),
        }
    }
}

struct DatasetSession {
    conn: Connection,
    source_path: PathBuf,
    file_size_bytes: u64,
    schema: Vec<ColumnSchema>,
    total_rows: usize,
    is_edit_mode: bool,
}

impl DatasetSession {
    fn load(path: PathBuf) -> Result<Self> {
        let file_size_bytes = fs::metadata(&path)
            .with_context(|| format!("Failed to read metadata for {}", path.display()))?
            .len();

        let conn = Connection::open_in_memory().context("Failed to start embedded DuckDB.")?;
        create_source_view(&conn, &path)?;

        let schema = load_schema(&conn)?;
        let total_rows = load_total_rows(&conn, SOURCE_VIEW)?;

        Ok(Self {
            conn,
            source_path: path,
            file_size_bytes,
            schema,
            total_rows,
            is_edit_mode: false,
        })
    }

    fn begin_edit(&mut self) -> Result<()> {
        if self.is_edit_mode {
            return Ok(());
        }

        self.conn
            .execute_batch(&format!(
                "DROP TABLE IF EXISTS {EDIT_TABLE};
                 CREATE TABLE {EDIT_TABLE} AS
                 SELECT row_number() OVER () AS __row_id, * FROM {SOURCE_VIEW};"
            ))
            .context("Failed to prepare an editable working copy.")?;

        self.is_edit_mode = true;
        Ok(())
    }

    fn cancel_edit(&mut self) -> Result<()> {
        self.conn
            .execute_batch(&format!("DROP TABLE IF EXISTS {EDIT_TABLE};"))
            .context("Failed to discard the editable working copy.")?;

        self.is_edit_mode = false;
        Ok(())
    }

    fn update_cell(
        &mut self,
        row_key: i64,
        column_name: &str,
        new_value: &str,
        set_null: bool,
    ) -> Result<()> {
        if !self.is_edit_mode {
            bail!("Enter edit mode before changing values.");
        }

        let column = self
            .schema
            .iter()
            .find(|column| column.name == column_name)
            .with_context(|| format!("Unknown column {column_name}"))?;

        let column_ident = quote_identifier(&column.name);

        if set_null {
            let sql = format!("UPDATE {EDIT_TABLE} SET {column_ident} = NULL WHERE __row_id = ?");
            self.conn
                .execute(&sql, [row_key])
                .with_context(|| format!("Failed to update {}", column.name))?;
        } else {
            let sql = format!(
                "UPDATE {EDIT_TABLE}
                 SET {column_ident} = CAST(? AS {})
                 WHERE __row_id = ?",
                column.declared_type
            );

            self.conn
                .execute(&sql, params![new_value, row_key])
                .with_context(|| format!("Failed to update {}", column.name))?;
        }

        Ok(())
    }

    fn save_to(&mut self, destination: &Path) -> Result<()> {
        if !self.is_edit_mode {
            bail!("There are no pending edits to save.");
        }

        let temp_path = temporary_save_path(destination);
        let select_columns = self
            .schema
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");

        let copy_sql = format!(
            "COPY (
                SELECT {select_columns}
                FROM {EDIT_TABLE}
                ORDER BY __row_id
            ) TO {} (FORMAT PARQUET, COMPRESSION SNAPPY)",
            sql_string(destination_to_string(&temp_path))
        );

        self.conn
            .execute_batch(&copy_sql)
            .context("DuckDB failed to write the parquet file.")?;

        self.conn
            .execute_batch(&format!("DROP VIEW IF EXISTS {SOURCE_VIEW};"))
            .context("Failed to release the previous parquet file handle.")?;

        replace_file_on_disk(&temp_path, destination)?;
        create_source_view(&self.conn, destination)?;

        self.conn
            .execute_batch(&format!("DROP TABLE IF EXISTS {EDIT_TABLE};"))
            .context("Failed to clear the editable working copy after save.")?;

        self.source_path = destination.to_path_buf();
        self.file_size_bytes = fs::metadata(destination)
            .with_context(|| format!("Failed to read metadata for {}", destination.display()))?
            .len();
        self.schema = load_schema(&self.conn)?;
        self.total_rows = load_total_rows(&self.conn, SOURCE_VIEW)?;
        self.is_edit_mode = false;

        Ok(())
    }

    fn snapshot(&self, query: &QuerySpec) -> Result<DatasetSnapshot> {
        let relation = if self.is_edit_mode { EDIT_TABLE } else { SOURCE_VIEW };
        let custom_sql = query
            .sql_query
            .as_deref()
            .map(str::trim)
            .filter(|sql| !sql.is_empty());

        if custom_sql.is_some() && self.is_edit_mode {
            bail!("Custom SQL results are read-only. Clear the SQL query before editing.");
        }

        let where_clause = if custom_sql.is_some() {
            String::new()
        } else {
            build_where_clause(&query.filters, &self.schema)
        };
        let order_clause = build_order_clause(query.sort.as_ref(), self.is_edit_mode);
        let (limit_clause, offset) = if query.page_size == 0 {
            (String::new(), 0)
        } else {
            let limit = query.page_size;
            (
                format!(
                    "LIMIT {limit}
                     OFFSET {}",
                    query.page_index.saturating_mul(limit)
                ),
                query.page_index.saturating_mul(limit),
            )
        };
        let rows_sql = if let Some(sql) = custom_sql {
            validate_custom_sql(sql)?;
            let base_query = wrap_custom_sql(relation, sql);
            format!(
                "SELECT *
                 FROM ({base_query}) AS parquetpp_sql
                 {order_clause}
                 {limit_clause}"
            )
        } else {
            let select_prefix = if self.is_edit_mode { "__row_id, " } else { "" };
            let select_columns = self
                .schema
                .iter()
                .map(|column| quote_identifier(&column.name))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT {select_prefix}{select_columns}
                 FROM {relation}
                 {where_clause}
                 {order_clause}
                 {limit_clause}"
            )
        };

        let visible_schema = if custom_sql.is_some() {
            let base_query = wrap_custom_sql(relation, custom_sql.unwrap_or_default());
            schema_from_query(&self.conn, &base_query)?
        } else {
            self.schema.clone()
        };
        let filtered_rows = if let Some(sql) = custom_sql {
            let base_query = wrap_custom_sql(relation, sql);
            count_wrapped_query(&self.conn, &base_query)?
        } else {
            count_rows_with_where(&self.conn, relation, &where_clause)?
        };
        let mut statement = self
            .conn
            .prepare(&rows_sql)
            .with_context(|| format!("Failed to prepare data query: {rows_sql}"))?;
        let mut rows = statement.query([]).context("Failed to fetch table rows.")?;

        let mut page_rows = Vec::new();
        while let Some(row) = rows.next().context("Failed while reading a row from DuckDB.")? {
            let row_key = if self.is_edit_mode {
                row.get::<_, i64>(0).context("Missing working row id.")?
            } else {
                (offset + page_rows.len() + 1) as i64
            };

            let value_offset = usize::from(self.is_edit_mode);
            let mut cells = Vec::with_capacity(visible_schema.len());

            for index in 0..visible_schema.len() {
                let value = row.get_ref(index + value_offset)?;
                cells.push(cell_value_from_ref(value));
            }

            page_rows.push(RowData { row_key, cells });
        }

        Ok(DatasetSnapshot {
            source_path: self.source_path.clone(),
            file_size_bytes: self.file_size_bytes,
            schema: visible_schema,
            rows: page_rows,
            total_rows: self.total_rows,
            filtered_rows,
            is_edit_mode: self.is_edit_mode,
        })
    }
}

fn create_source_view(conn: &Connection, path: &Path) -> Result<()> {
    let file_sql = sql_string(destination_to_string(path));
    let sql = format!(
        "DROP VIEW IF EXISTS {SOURCE_VIEW};
         CREATE VIEW {SOURCE_VIEW} AS
         SELECT * FROM read_parquet({file_sql});"
    );

    conn.execute_batch(&sql)
        .with_context(|| format!("Failed to open {}", path.display()))?;

    Ok(())
}

fn load_schema(conn: &Connection) -> Result<Vec<ColumnSchema>> {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info('{SOURCE_VIEW}')"))
        .context("Failed to inspect the parquet schema.")?;

    let columns = statement
        .query_map([], |row| {
            let name: String = row.get(1)?;
            let declared_type: String = row.get(2)?;
            Ok(ColumnSchema {
                name,
                category: categorize_type(&declared_type),
                declared_type,
            })
        })?
        .collect::<duckdb::Result<Vec<_>>>()
        .context("Failed to collect parquet schema information.")?;

    Ok(columns)
}

fn load_total_rows(conn: &Connection, relation: &str) -> Result<usize> {
    count_rows_with_where(conn, relation, "")
}

fn count_wrapped_query(conn: &Connection, query_sql: &str) -> Result<usize> {
    let sql = format!("SELECT COUNT(*) FROM ({query_sql}) AS parquetpp_count");
    let total = conn
        .query_row(&sql, [], |row| row.get::<_, i64>(0))
        .with_context(|| format!("Failed to count rows using query: {sql}"))?;
    usize::try_from(total).context("DuckDB returned a negative row count.")
}

fn count_rows_with_where(conn: &Connection, relation: &str, where_clause: &str) -> Result<usize> {
    let sql = format!("SELECT COUNT(*) FROM {relation} {where_clause}");
    let total = conn
        .query_row(&sql, [], |row| row.get::<_, i64>(0))
        .with_context(|| format!("Failed to count rows using query: {sql}"))?;
    usize::try_from(total).context("DuckDB returned a negative row count.")
}

fn schema_from_statement(statement: &Statement<'_>) -> Result<Vec<ColumnSchema>> {
    let mut schema = Vec::with_capacity(statement.column_count());
    for index in 0..statement.column_count() {
        let name = statement
            .column_name(index)
            .context("Failed to read SQL result column name.")?
            .clone();
        let declared_type = statement.column_type(index).to_string();
        schema.push(ColumnSchema {
            name,
            category: categorize_type(&declared_type),
            declared_type,
        });
    }
    Ok(schema)
}

fn schema_from_query(conn: &Connection, query_sql: &str) -> Result<Vec<ColumnSchema>> {
    let schema_sql = format!("SELECT * FROM ({query_sql}) AS parquetpp_schema LIMIT 0");
    let mut statement = conn
        .prepare(&schema_sql)
        .with_context(|| format!("Failed to inspect SQL result schema: {schema_sql}"))?;
    let _ = statement
        .query([])
        .context("Failed to execute SQL schema inspection query.")?;
    schema_from_statement(&statement)
}

fn validate_custom_sql(sql: &str) -> Result<()> {
    let normalized = sql.trim_start().to_ascii_lowercase();
    if normalized.starts_with("select ") || normalized.starts_with("with ") {
        Ok(())
    } else {
        bail!("SQL must start with SELECT or WITH and should query the virtual table named data.")
    }
}

fn wrap_custom_sql(relation: &str, sql: &str) -> String {
    format!("WITH data AS (SELECT * FROM {relation}) {sql}")
}

fn build_where_clause(filters: &[FilterRule], schema: &[ColumnSchema]) -> String {
    if filters.is_empty() {
        return String::new();
    }

    let clauses = filters
        .iter()
        .filter_map(|filter| {
            schema
                .iter()
                .find(|column| column.name == filter.column_name)
                .map(|column| filter_clause(filter, column))
        })
        .collect::<Vec<_>>();

    if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    }
}

fn filter_clause(filter: &FilterRule, column: &ColumnSchema) -> String {
    let column_ident = quote_identifier(&column.name);
    let value = filter.value.trim();

    match filter.operator {
        FilterOperator::Contains => format!(
            "CAST({column_ident} AS VARCHAR) ILIKE {}",
            sql_string(format!("%{value}%"))
        ),
        FilterOperator::StartsWith => format!(
            "CAST({column_ident} AS VARCHAR) ILIKE {}",
            sql_string(format!("{value}%"))
        ),
        FilterOperator::Equals => equality_clause(&column_ident, column, value, "="),
        FilterOperator::NotEquals => equality_clause(&column_ident, column, value, "!="),
        FilterOperator::GreaterThan => comparison_clause(&column_ident, column, value, ">"),
        FilterOperator::GreaterThanOrEqual => comparison_clause(&column_ident, column, value, ">="),
        FilterOperator::LessThan => comparison_clause(&column_ident, column, value, "<"),
        FilterOperator::LessThanOrEqual => comparison_clause(&column_ident, column, value, "<="),
        FilterOperator::IsNull => format!("{column_ident} IS NULL"),
        FilterOperator::IsNotNull => format!("{column_ident} IS NOT NULL"),
    }
}

fn equality_clause(column_ident: &str, column: &ColumnSchema, value: &str, operator: &str) -> String {
    match column.category {
        ColumnCategory::Text | ColumnCategory::Other => {
            format!("CAST({column_ident} AS VARCHAR) {operator} {}", sql_string(value))
        }
        _ => format!(
            "{column_ident} {operator} CAST({} AS {})",
            sql_string(value),
            column.declared_type
        ),
    }
}

fn comparison_clause(column_ident: &str, column: &ColumnSchema, value: &str, operator: &str) -> String {
    match column.category {
        ColumnCategory::Text | ColumnCategory::Other => {
            format!("CAST({column_ident} AS VARCHAR) {operator} {}", sql_string(value))
        }
        _ => format!(
            "{column_ident} {operator} CAST({} AS {})",
            sql_string(value),
            column.declared_type
        ),
    }
}

fn build_order_clause(sort: Option<&SortSpec>, is_edit_mode: bool) -> String {
    match sort {
        Some(sort_spec) => {
            let direction = match sort_spec.direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };

            format!("ORDER BY {} {direction}", quote_identifier(&sort_spec.column_name))
        }
        None if is_edit_mode => "ORDER BY __row_id ASC".to_owned(),
        None => String::new(),
    }
}

fn categorize_type(declared_type: &str) -> ColumnCategory {
    let upper = declared_type.trim().to_ascii_uppercase();

    if upper.contains("BOOL") {
        ColumnCategory::Boolean
    } else if [
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "INT",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "FLOAT",
        "DOUBLE",
        "REAL",
        "DECIMAL",
    ]
    .iter()
    .any(|marker| upper.contains(marker))
    {
        ColumnCategory::Number
    } else if ["DATE", "TIME", "TIMESTAMP"].iter().any(|marker| upper.contains(marker)) {
        ColumnCategory::Temporal
    } else if ["VARCHAR", "TEXT", "STRING", "JSON"].iter().any(|marker| upper.contains(marker)) {
        ColumnCategory::Text
    } else {
        ColumnCategory::Other
    }
}

fn cell_value_from_ref(value: ValueRef<'_>) -> CellValue {
    match value {
        ValueRef::Null => CellValue::Null,
        ValueRef::Boolean(value) => CellValue::Bool(value),
        ValueRef::TinyInt(value) => CellValue::Text(value.to_string()),
        ValueRef::SmallInt(value) => CellValue::Text(value.to_string()),
        ValueRef::Int(value) => CellValue::Text(value.to_string()),
        ValueRef::BigInt(value) => CellValue::Text(value.to_string()),
        ValueRef::HugeInt(value) => CellValue::Text(value.to_string()),
        ValueRef::UTinyInt(value) => CellValue::Text(value.to_string()),
        ValueRef::USmallInt(value) => CellValue::Text(value.to_string()),
        ValueRef::UInt(value) => CellValue::Text(value.to_string()),
        ValueRef::UBigInt(value) => CellValue::Text(value.to_string()),
        ValueRef::Float(value) => CellValue::Text(value.to_string()),
        ValueRef::Double(value) => CellValue::Text(value.to_string()),
        ValueRef::Decimal(value) => CellValue::Text(value.to_string()),
        ValueRef::Timestamp(unit, value) => CellValue::Text(format_timestamp(unit, value)),
        ValueRef::Text(bytes) => CellValue::Text(String::from_utf8_lossy(bytes).into_owned()),
        ValueRef::Blob(bytes) => CellValue::Text(format!("Binary({} bytes)", bytes.len())),
        ValueRef::Date32(value) => CellValue::Text(format_date32(value)),
        ValueRef::Time64(unit, value) => CellValue::Text(format_time64(unit, value)),
        ValueRef::Interval { months, days, nanos } => {
            CellValue::Text(format!("Interval({months} months, {days} days, {nanos} nanos)"))
        }
        other => CellValue::Text(format!("{other:?}")),
    }
}

fn format_date32(days_since_epoch: i32) -> String {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid unix epoch");
    epoch
        .checked_add_signed(ChronoDuration::days(i64::from(days_since_epoch)))
        .map(|date| date.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| days_since_epoch.to_string())
}

fn format_timestamp(unit: TimeUnit, raw_value: i64) -> String {
    let (seconds, nanos) = match unit {
        TimeUnit::Second => (raw_value, 0),
        TimeUnit::Millisecond => (
            raw_value.div_euclid(1_000),
            (raw_value.rem_euclid(1_000) * 1_000_000) as u32,
        ),
        TimeUnit::Microsecond => (
            raw_value.div_euclid(1_000_000),
            (raw_value.rem_euclid(1_000_000) * 1_000) as u32,
        ),
        TimeUnit::Nanosecond => (
            raw_value.div_euclid(1_000_000_000),
            raw_value.rem_euclid(1_000_000_000) as u32,
        ),
    };

    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .map(|value| value.naive_utc().format("%Y-%m-%d %H:%M:%S%.f").to_string())
        .unwrap_or_else(|| raw_value.to_string())
}

fn format_time64(unit: TimeUnit, raw_value: i64) -> String {
    let nanos_since_midnight = match unit {
        TimeUnit::Second => raw_value.saturating_mul(1_000_000_000),
        TimeUnit::Millisecond => raw_value.saturating_mul(1_000_000),
        TimeUnit::Microsecond => raw_value.saturating_mul(1_000),
        TimeUnit::Nanosecond => raw_value,
    };

    let day_nanos = 86_400_i64 * 1_000_000_000;
    let normalized = nanos_since_midnight.rem_euclid(day_nanos);
    let seconds = normalized / 1_000_000_000;
    let nanos = (normalized % 1_000_000_000) as u32;

    NaiveTime::from_num_seconds_from_midnight_opt(seconds as u32, nanos)
        .map(|value| value.format("%H:%M:%S%.f").to_string())
        .unwrap_or_else(|| raw_value.to_string())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn sql_string(value: impl AsRef<str>) -> String {
    format!("'{}'", value.as_ref().replace('\'', "''"))
}

fn destination_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn temporary_save_path(destination: &Path) -> PathBuf {
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("data.parquet");
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let temp_file_name = format!(".{file_name}.parquetpp-{timestamp}.tmp");

    destination
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(temp_file_name)
}

fn replace_file_on_disk(temp_path: &Path, destination: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::rename(temp_path, destination).with_context(|| {
            format!(
                "Failed to move {} into place at {}",
                temp_path.display(),
                destination.display()
            )
        })?;
    }

    #[cfg(not(unix))]
    {
        if destination.exists() {
            fs::remove_file(destination).with_context(|| {
                format!("Failed to replace existing file {}", destination.display())
            })?;
        }

        fs::rename(temp_path, destination).with_context(|| {
            format!(
                "Failed to move {} into place at {}",
                temp_path.display(),
                destination.display()
            )
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn sample_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("sample-data")
            .join(name)
    }

    fn test_output_path(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("parquetpp-native-{suffix}-{name}"))
    }

    #[test]
    fn loads_sample_schema_and_rows() -> Result<()> {
        let session = DatasetSession::load(sample_path("people-small.parquet"))?;
        let snapshot = session.snapshot(&QuerySpec {
            page_index: 0,
            page_size: 25,
            sort: None,
            filters: Vec::new(),
            sql_query: None,
        })?;

        assert_eq!(snapshot.total_rows, 500);
        assert_eq!(snapshot.filtered_rows, 500);
        assert_eq!(snapshot.rows.len(), 25);
        assert!(snapshot.schema.iter().any(|column| column.name == "first_name"));
        assert!(!snapshot.is_edit_mode);

        Ok(())
    }

    #[test]
    fn edits_and_saves_a_new_parquet_file() -> Result<()> {
        let mut session = DatasetSession::load(sample_path("people-small.parquet"))?;
        session.begin_edit()?;
        session.update_cell(1, "first_name", "Zelda", false)?;

        let save_path = test_output_path("people-edited.parquet");
        session.save_to(&save_path)?;

        let conn = Connection::open_in_memory()?;
        let sql = format!(
            "SELECT first_name FROM read_parquet({}) LIMIT 1",
            sql_string(destination_to_string(&save_path))
        );
        let saved_name: String = conn.query_row(&sql, [], |row| row.get(0))?;
        assert_eq!(saved_name, "Zelda");

        let _ = fs::remove_file(save_path);
        Ok(())
    }

    #[test]
    fn formats_dates_for_display() {
        assert_eq!(format_date32(18628), "2021-01-01");
    }
}
