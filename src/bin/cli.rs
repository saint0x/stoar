use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use stoar::{AliasRecord, NamespaceConfig, SqlRows, Store};
use tokio::io::AsyncWriteExt;

#[derive(Parser)]
#[command(name = "stoar", version, about = "SQLite + CAS object store")]
struct Cli {
    #[arg(long, short, global = true, default_value = "stoar.db")]
    db: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Sql {
        sql: String,
        params: Vec<String>,
        #[arg(long, value_enum, default_value_t = SqlMode::Table)]
        mode: SqlMode,
    },
    Cas {
        #[command(subcommand)]
        command: CasCommand,
    },
    Alias {
        #[command(subcommand)]
        command: AliasCommand,
    },
    Ref {
        #[command(subcommand)]
        command: RefCommand,
    },
    Gc {
        #[command(subcommand)]
        command: GcCommand,
    },
    Verify {
        #[arg(long)]
        all: bool,
        content_id: Option<String>,
    },
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },
    Sync {
        #[command(subcommand)]
        command: SyncCommand,
    },
    Namespace {
        #[command(subcommand)]
        command: NamespaceCommand,
    },
    Serve {
        #[arg(long, default_value = "127.0.0.1:7777")]
        listen: String,
        #[arg(long)]
        token: String,
        #[arg(long, default_value_t = false)]
        allow_sql_mutations: bool,
    },
}

#[derive(Subcommand)]
enum NamespaceCommand {
    Set {
        namespace: String,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        read_only: bool,
        #[arg(long)]
        max_objects: Option<i64>,
        #[arg(long)]
        max_bytes: Option<i64>,
    },
    Get {
        namespace: String,
    },
    List,
}

#[derive(Subcommand)]
enum CasCommand {
    Put {
        input: String,
        #[arg(long)]
        mime: Option<String>,
    },
    Get {
        content_id: String,
        #[arg(long, short)]
        output: Option<PathBuf>,
        #[arg(long)]
        offset: Option<usize>,
        #[arg(long)]
        length: Option<usize>,
    },
    Head {
        content_id: String,
    },
}

#[derive(Subcommand)]
enum AliasCommand {
    Set {
        namespace: String,
        alias: String,
        content_id: String,
        #[arg(long)]
        expected_version: Option<i64>,
    },
    Get {
        namespace: String,
        alias: String,
    },
    Delete {
        namespace: String,
        alias: String,
    },
}

#[derive(Subcommand)]
enum RefCommand {
    Inc { content_id: String },
    Dec { content_id: String },
    Pin { content_id: String },
    Unpin { content_id: String },
}

#[derive(Subcommand)]
enum GcCommand {
    Run {
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum SnapshotCommand {
    Create { output: PathBuf },
}

#[derive(Subcommand)]
enum SyncCommand {
    Export { output: PathBuf },
    Import { input: PathBuf },
}

#[derive(Copy, Clone, Eq, PartialEq, ValueEnum)]
enum SqlMode {
    Table,
    Json,
    Raw,
}

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
    admin_token: String,
    allow_sql_mutations: bool,
    requests_total: Arc<AtomicU64>,
    auth_failures_total: Arc<AtomicU64>,
}

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
    params: Vec<String>,
}

#[derive(Deserialize)]
struct AliasSetRequest {
    content_id: String,
    expected_version: Option<i64>,
}

#[derive(Deserialize)]
struct NamespaceUpsertRequest {
    token: Option<String>,
    read_only: Option<bool>,
    max_objects: Option<i64>,
    max_bytes: Option<i64>,
}

#[derive(Deserialize)]
struct VerifyQuery {
    all: Option<bool>,
}

#[derive(Deserialize)]
struct GcQuery {
    dry_run: Option<bool>,
}

#[derive(Serialize)]
struct Health {
    ok: bool,
}

enum Principal {
    Admin,
    Namespace,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> stoar::Result<()> {
    let store = Store::open(&cli.db)?;

    match cli.command {
        Command::Sql { sql, params, mode } => run_sql(&store, &sql, params, mode)?,
        Command::Cas { command } => match command {
            CasCommand::Put { input, mime } => {
                let cid = if input == "-" {
                    let mut stdin = std::io::stdin();
                    store.put_content_reader(&mut stdin, mime.as_deref())?
                } else {
                    store.put_content_file(input, mime.as_deref())?
                };
                println!("{cid}");
            }
            CasCommand::Get {
                content_id,
                output,
                offset,
                length,
            } => {
                let mut writer: Box<dyn Write> = if let Some(path) = output {
                    Box::new(std::fs::File::create(path)?)
                } else {
                    Box::new(std::io::stdout())
                };

                if offset.is_some() || length.is_some() {
                    let data = store.read_range(
                        &content_id,
                        offset.unwrap_or(0),
                        length.unwrap_or(usize::MAX),
                    )?;
                    match data {
                        Some(v) => writer.write_all(&v)?,
                        None => {
                            eprintln!("not found: {content_id}");
                            std::process::exit(1);
                        }
                    }
                } else if !store.stream_content_to_writer(&content_id, &mut writer)? {
                    eprintln!("not found: {content_id}");
                    std::process::exit(1);
                }
            }
            CasCommand::Head { content_id } => match store.head_content(&content_id)? {
                Some(v) => println!("{}", serde_json::to_string_pretty(&v)?),
                None => {
                    eprintln!("not found: {content_id}");
                    std::process::exit(1);
                }
            },
        },
        Command::Alias { command } => match command {
            AliasCommand::Set {
                namespace,
                alias,
                content_id,
                expected_version,
            } => {
                let out = store.set_alias(&namespace, &alias, &content_id, expected_version)?;
                print_alias(&out)?;
            }
            AliasCommand::Get { namespace, alias } => {
                match store.resolve_alias(&namespace, &alias)? {
                    Some(v) => print_alias(&v)?,
                    None => {
                        eprintln!("not found: {namespace}/{alias}");
                        std::process::exit(1);
                    }
                }
            }
            AliasCommand::Delete { namespace, alias } => {
                let removed = store.remove_alias(&namespace, &alias)?;
                if !removed {
                    eprintln!("not found: {namespace}/{alias}");
                    std::process::exit(1);
                }
            }
        },
        Command::Ref { command } => match command {
            RefCommand::Inc { content_id } => store.inc_ref(&content_id)?,
            RefCommand::Dec { content_id } => store.dec_ref(&content_id)?,
            RefCommand::Pin { content_id } => store.pin(&content_id)?,
            RefCommand::Unpin { content_id } => store.unpin(&content_id)?,
        },
        Command::Gc { command } => match command {
            GcCommand::Run { dry_run } => {
                println!("{}", serde_json::to_string_pretty(&store.gc_run(dry_run)?)?);
            }
        },
        Command::Verify { all, content_id } => {
            if all {
                let results = store.verify_all()?;
                println!("{}", serde_json::to_string_pretty(&results)?);
                if results.iter().any(|r| !r.ok) {
                    std::process::exit(1);
                }
            } else {
                let cid = content_id.ok_or_else(|| {
                    stoar::StoreError::InvalidConfig(
                        "content_id is required unless --all".to_string(),
                    )
                })?;
                let result = store.verify_content(&cid)?;
                println!("{}", serde_json::to_string_pretty(&result)?);
                if !result.ok {
                    std::process::exit(1);
                }
            }
        }
        Command::Snapshot { command } => match command {
            SnapshotCommand::Create { output } => store.snapshot_create(output)?,
        },
        Command::Sync { command } => match command {
            SyncCommand::Export { output } => store.sync_export(output)?,
            SyncCommand::Import { input } => store.sync_import(input)?,
        },
        Command::Namespace { command } => match command {
            NamespaceCommand::Set {
                namespace,
                token,
                read_only,
                max_objects,
                max_bytes,
            } => {
                let ns = store.upsert_namespace(
                    &namespace,
                    token.as_deref(),
                    read_only,
                    max_objects,
                    max_bytes,
                )?;
                println!("{}", serde_json::to_string_pretty(&ns)?);
            }
            NamespaceCommand::Get { namespace } => match store.get_namespace(&namespace)? {
                Some(ns) => println!("{}", serde_json::to_string_pretty(&ns)?),
                None => {
                    eprintln!("not found: namespace {namespace}");
                    std::process::exit(1);
                }
            },
            NamespaceCommand::List => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&store.list_namespaces()?)?
                );
            }
        },
        Command::Serve {
            listen,
            token,
            allow_sql_mutations,
        } => {
            serve(Arc::new(store), listen, token, allow_sql_mutations).await?;
        }
    }

    Ok(())
}

fn run_sql(store: &Store, sql: &str, params: Vec<String>, mode: SqlMode) -> stoar::Result<()> {
    let parsed: Vec<Box<dyn rusqlite::ToSql>> = params
        .iter()
        .map(|p| -> Box<dyn rusqlite::ToSql> {
            if let Ok(i) = p.parse::<i64>() {
                return Box::new(i);
            }
            if let Ok(f) = p.parse::<f64>() {
                return Box::new(f);
            }
            Box::new(p.clone())
        })
        .collect();
    let refs: Vec<&dyn rusqlite::ToSql> = parsed.iter().map(|b| b.as_ref()).collect();
    let params_json = serde_json::to_string(&params)?;

    if looks_like_select(sql) {
        let rows = store.query_sql_as("cli", sql, &refs, Some(&params_json))?;
        match mode {
            SqlMode::Json => println!("{}", serde_json::to_string_pretty(&rows)?),
            SqlMode::Raw => print_raw(&rows),
            SqlMode::Table => print_table(&rows),
        }
    } else {
        let changed = store.execute_sql_as("cli", sql, &refs, Some(&params_json))?;
        println!("{changed}");
    }
    Ok(())
}

async fn serve(
    store: Arc<Store>,
    listen: String,
    admin_token: String,
    allow_sql_mutations: bool,
) -> stoar::Result<()> {
    let state = AppState {
        store,
        admin_token,
        allow_sql_mutations,
        requests_total: Arc::new(AtomicU64::new(0)),
        auth_failures_total: Arc::new(AtomicU64::new(0)),
    };

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .route("/metrics", get(metrics))
        .route("/cas", post(admin_cas_put_http))
        .route("/cas/:content_id", get(admin_cas_get_http))
        .route("/cas/:content_id/head", get(admin_cas_head_http))
        .route(
            "/alias/:namespace/:alias",
            get(admin_alias_get_http)
                .post(admin_alias_set_http)
                .delete(admin_alias_delete_http),
        )
        .route("/ref/:action/:content_id", post(admin_ref_http))
        .route("/sql", post(sql_http))
        .route("/gc", post(gc_http))
        .route("/verify", post(verify_http))
        .route("/admin/namespaces", get(namespaces_list_http))
        .route("/admin/namespace/:namespace", post(namespace_upsert_http))
        .route("/ns/:namespace/cas", post(ns_cas_put_http))
        .route("/ns/:namespace/cas/:content_id", get(ns_cas_get_http))
        .route("/ns/:namespace/cas/:content_id/head", get(ns_cas_head_http))
        .route(
            "/ns/:namespace/alias/:alias",
            get(ns_alias_get_http).post(ns_alias_set_http),
        )
        .route("/ns/:namespace/ref/:action/:content_id", post(ns_ref_http))
        .route("/ns/:namespace/verify", post(ns_verify_http))
        .layer(DefaultBodyLimit::disable())
        .with_state(state);

    let addr: SocketAddr = listen
        .parse()
        .map_err(|e| stoar::StoreError::InvalidConfig(format!("invalid listen addr: {e}")))?;

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| stoar::StoreError::InvalidConfig(format!("failed to bind socket: {e}")))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| stoar::StoreError::InvalidConfig(format!("server failure: {e}")))?;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn health() -> Json<Health> {
    Json(Health { ok: true })
}

async fn metrics(State(state): State<AppState>) -> String {
    format!(
        "stoar_requests_total {}\nstoar_auth_failures_total {}\n",
        state.requests_total.load(Ordering::Relaxed),
        state.auth_failures_total.load(Ordering::Relaxed),
    )
}

async fn admin_cas_put_http(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_admin(&state, &headers) {
        return resp;
    }

    let mime = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let temp_path = match stream_body_to_temp_file(body).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let store = state.store.clone();
    let mime_owned = mime.clone();
    match tokio::task::spawn_blocking(move || -> stoar::Result<String> {
        let cid = store.put_content_file(&temp_path, mime_owned.as_deref())?;
        let _ = std::fs::remove_file(&temp_path);
        Ok(cid)
    })
    .await
    {
        Ok(Ok(cid)) => Json(serde_json::json!({"content_id": cid})).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("upload join error: {e}"),
        )
            .into_response(),
    }
}

async fn admin_cas_get_http(
    State(state): State<AppState>,
    AxumPath(content_id): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_admin(&state, &headers) {
        return resp;
    }

    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    if let Some(r) = range {
        if let Some((start, end)) = parse_range(r) {
            return match state
                .store
                .read_range(&content_id, start, end.saturating_sub(start) + 1)
            {
                Ok(Some(bytes)) => {
                    let mut resp = Response::new(bytes.into());
                    *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                    resp
                }
                Ok(None) => StatusCode::NOT_FOUND.into_response(),
                Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
            };
        }
    }

    match state.store.get_content(&content_id) {
        Ok(Some(bytes)) => bytes.into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn admin_cas_head_http(
    State(state): State<AppState>,
    AxumPath(content_id): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_admin(&state, &headers) {
        return resp;
    }
    match state.store.head_content(&content_id) {
        Ok(Some(meta)) => Json(meta).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn admin_alias_set_http(
    State(state): State<AppState>,
    AxumPath((namespace, alias)): AxumPath<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<AliasSetRequest>,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_admin(&state, &headers) {
        return resp;
    }
    match state
        .store
        .set_alias(&namespace, &alias, &body.content_id, body.expected_version)
    {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn admin_alias_get_http(
    State(state): State<AppState>,
    AxumPath((namespace, alias)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_admin(&state, &headers) {
        return resp;
    }
    match state.store.resolve_alias(&namespace, &alias) {
        Ok(Some(v)) => Json(v).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn admin_alias_delete_http(
    State(state): State<AppState>,
    AxumPath((namespace, alias)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_admin(&state, &headers) {
        return resp;
    }
    match state.store.remove_alias(&namespace, &alias) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn admin_ref_http(
    State(state): State<AppState>,
    AxumPath((action, content_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_admin(&state, &headers) {
        return resp;
    }
    let result = match action.as_str() {
        "inc" => state.store.inc_ref(&content_id),
        "dec" => state.store.dec_ref(&content_id),
        "pin" => state.store.pin(&content_id),
        "unpin" => state.store.unpin(&content_id),
        _ => return (StatusCode::BAD_REQUEST, "unknown action").into_response(),
    };
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn namespace_upsert_http(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Json(body): Json<NamespaceUpsertRequest>,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_admin(&state, &headers) {
        return resp;
    }

    match state.store.upsert_namespace(
        &namespace,
        body.token.as_deref(),
        body.read_only.unwrap_or(false),
        body.max_objects,
        body.max_bytes,
    ) {
        Ok(ns) => Json(ns).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn namespaces_list_http(State(state): State<AppState>, headers: HeaderMap) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_admin(&state, &headers) {
        return resp;
    }
    match state.store.list_namespaces() {
        Ok(list) => Json(list).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    }
}

async fn ns_cas_put_http(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_namespace(&state, &headers, &namespace, true).await {
        return resp;
    }

    let mime = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let temp_path = match stream_body_to_temp_file(body).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let store = state.store.clone();
    let ns = namespace.clone();
    let mime_owned = mime.clone();
    let cid_result = tokio::task::spawn_blocking(move || -> stoar::Result<String> {
        let cid = store.put_content_file(&temp_path, mime_owned.as_deref())?;
        store.attach_content_to_namespace(&ns, &cid)?;
        let _ = std::fs::remove_file(&temp_path);
        Ok(cid)
    })
    .await;

    match cid_result {
        Ok(Ok(cid)) => Json(serde_json::json!({"content_id": cid})).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("upload join error: {e}"),
        )
            .into_response(),
    }
}

async fn ns_cas_get_http(
    State(state): State<AppState>,
    AxumPath((namespace, content_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_namespace(&state, &headers, &namespace, false).await {
        return resp;
    }
    match state.store.namespace_has_content(&namespace, &content_id) {
        Ok(true) => {}
        Ok(false) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }

    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    if let Some(r) = range {
        if let Some((start, end)) = parse_range(r) {
            return match state
                .store
                .read_range(&content_id, start, end.saturating_sub(start) + 1)
            {
                Ok(Some(bytes)) => {
                    let mut resp = Response::new(bytes.into());
                    *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                    resp
                }
                Ok(None) => StatusCode::NOT_FOUND.into_response(),
                Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
            };
        }
    }

    match state.store.get_content(&content_id) {
        Ok(Some(bytes)) => bytes.into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn ns_cas_head_http(
    State(state): State<AppState>,
    AxumPath((namespace, content_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_namespace(&state, &headers, &namespace, false).await {
        return resp;
    }
    match state.store.namespace_has_content(&namespace, &content_id) {
        Ok(true) => {}
        Ok(false) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }

    match state.store.head_content(&content_id) {
        Ok(Some(meta)) => Json(meta).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn ns_alias_set_http(
    State(state): State<AppState>,
    AxumPath((namespace, alias)): AxumPath<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<AliasSetRequest>,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_namespace(&state, &headers, &namespace, true).await {
        return resp;
    }

    match state
        .store
        .set_alias(&namespace, &alias, &body.content_id, body.expected_version)
    {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn ns_alias_get_http(
    State(state): State<AppState>,
    AxumPath((namespace, alias)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_namespace(&state, &headers, &namespace, false).await {
        return resp;
    }

    match state.store.resolve_alias(&namespace, &alias) {
        Ok(Some(v)) => Json(v).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn ns_ref_http(
    State(state): State<AppState>,
    AxumPath((namespace, action, content_id)): AxumPath<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_namespace(&state, &headers, &namespace, true).await {
        return resp;
    }

    match state.store.namespace_has_content(&namespace, &content_id) {
        Ok(true) => {}
        Ok(false) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }

    let result = match action.as_str() {
        "inc" => state.store.inc_ref(&content_id),
        "dec" => state.store.dec_ref(&content_id),
        "pin" => state.store.pin(&content_id),
        "unpin" => state.store.unpin(&content_id),
        _ => return (StatusCode::BAD_REQUEST, "unknown action").into_response(),
    };

    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn ns_verify_http(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    query: Query<VerifyQuery>,
    body: Option<Json<HashMap<String, String>>>,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_namespace(&state, &headers, &namespace, false).await {
        return resp;
    }

    if query.all.unwrap_or(false) {
        let rows = match state.store.query_sql(
            "SELECT content_id FROM cas_namespace_objects WHERE namespace = ? ORDER BY attached_at",
            &[&namespace],
        ) {
            Ok(v) => v,
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
        };

        let mut out = Vec::new();
        for r in rows.rows {
            if let Some(cid) = r.first().and_then(|v| v.as_str()) {
                match state.store.verify_content(cid) {
                    Ok(v) => out.push(v),
                    Err(e) => return (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
                }
            }
        }
        return Json(out).into_response();
    }

    let Some(Json(map)) = body else {
        return (StatusCode::BAD_REQUEST, "missing body").into_response();
    };
    let Some(cid) = map.get("content_id") else {
        return (StatusCode::BAD_REQUEST, "missing content_id").into_response();
    };

    match state.store.namespace_has_content(&namespace, cid) {
        Ok(true) => {}
        Ok(false) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }

    match state.store.verify_content(cid) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn verify_http(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: Query<VerifyQuery>,
    body: Option<Json<HashMap<String, String>>>,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_admin(&state, &headers) {
        return resp;
    }

    if query.all.unwrap_or(false) {
        return match state.store.verify_all() {
            Ok(v) => Json(v).into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
        };
    }

    let Some(Json(map)) = body else {
        return (StatusCode::BAD_REQUEST, "missing body").into_response();
    };
    let Some(cid) = map.get("content_id") else {
        return (StatusCode::BAD_REQUEST, "missing content_id").into_response();
    };

    match state.store.verify_content(cid) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    }
}

async fn gc_http(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: Query<GcQuery>,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_admin(&state, &headers) {
        return resp;
    }

    match state.store.gc_run(query.dry_run.unwrap_or(false)) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    }
}

async fn sql_http(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SqlRequest>,
) -> Response {
    state.requests_total.fetch_add(1, Ordering::Relaxed);
    if let Err(resp) = authorize_admin(&state, &headers) {
        return resp;
    }

    if !state.allow_sql_mutations && !looks_like_select(&body.sql) {
        return (StatusCode::FORBIDDEN, "mutating SQL disabled").into_response();
    }

    let parsed: Vec<Box<dyn rusqlite::ToSql>> = body
        .params
        .iter()
        .map(|p| -> Box<dyn rusqlite::ToSql> {
            if let Ok(i) = p.parse::<i64>() {
                return Box::new(i);
            }
            if let Ok(f) = p.parse::<f64>() {
                return Box::new(f);
            }
            Box::new(p.clone())
        })
        .collect();
    let refs: Vec<&dyn rusqlite::ToSql> = parsed.iter().map(|b| b.as_ref()).collect();
    let params_json = serde_json::to_string(&body.params).unwrap_or_else(|_| "[]".to_string());

    if looks_like_select(&body.sql) {
        match state
            .store
            .query_sql_as("server:admin", &body.sql, &refs, Some(&params_json))
        {
            Ok(rows) => Json(serde_json::json!({"rows": rows})).into_response(),
            Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
        }
    } else {
        match state
            .store
            .execute_sql_as("server:admin", &body.sql, &refs, Some(&params_json))
        {
            Ok(changed) => Json(serde_json::json!({"changed": changed})).into_response(),
            Err(e) => (StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
        }
    }
}

fn authorize_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> std::result::Result<Principal, Response> {
    let bearer = bearer_token(headers);
    if bearer == Some(state.admin_token.as_str()) {
        Ok(Principal::Admin)
    } else {
        state.auth_failures_total.fetch_add(1, Ordering::Relaxed);
        Err((StatusCode::UNAUTHORIZED, "unauthorized").into_response())
    }
}

async fn authorize_namespace(
    state: &AppState,
    headers: &HeaderMap,
    namespace: &str,
    require_write: bool,
) -> std::result::Result<Principal, Response> {
    let bearer = bearer_token(headers);
    if bearer == Some(state.admin_token.as_str()) {
        return Ok(Principal::Admin);
    }

    let Some(token) = bearer else {
        state.auth_failures_total.fetch_add(1, Ordering::Relaxed);
        return Err((StatusCode::UNAUTHORIZED, "unauthorized").into_response());
    };

    let valid = state
        .store
        .namespace_token_valid(namespace, token)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response())?;
    if !valid {
        state.auth_failures_total.fetch_add(1, Ordering::Relaxed);
        return Err((StatusCode::UNAUTHORIZED, "unauthorized").into_response());
    }

    if require_write {
        match state
            .store
            .get_namespace(namespace)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response())?
        {
            Some(NamespaceConfig {
                read_only: true, ..
            }) => return Err((StatusCode::FORBIDDEN, "namespace is read-only").into_response()),
            Some(_) => {}
            None => return Err((StatusCode::NOT_FOUND, "namespace not found").into_response()),
        }
    }

    Ok(Principal::Namespace)
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

fn looks_like_select(sql: &str) -> bool {
    let trimmed = sql.trim_start().to_ascii_lowercase();
    trimmed.starts_with("select")
        || trimmed.starts_with("with")
        || trimmed.starts_with("pragma")
        || trimmed.starts_with("explain")
}

fn print_alias(record: &AliasRecord) -> stoar::Result<()> {
    println!("{}", serde_json::to_string_pretty(record)?);
    Ok(())
}

fn print_raw(rows: &SqlRows) {
    for row in &rows.rows {
        let line = row.iter().map(cell_to_raw).collect::<Vec<_>>().join("\t");
        println!("{line}");
    }
}

fn print_table(rows: &SqlRows) {
    if rows.columns.is_empty() {
        println!("(ok)");
        return;
    }

    let mut widths = rows.columns.iter().map(|c| c.len()).collect::<Vec<_>>();
    for row in &rows.rows {
        for (idx, cell) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(cell_to_raw(cell).len());
        }
    }

    let header = rows
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{c:<w$}", w = widths[i]))
        .collect::<Vec<_>>()
        .join(" | ");
    println!("{header}");

    let sep = widths
        .iter()
        .map(|w| "-".repeat(*w))
        .collect::<Vec<_>>()
        .join("-+-");
    println!("{sep}");

    for row in &rows.rows {
        let line = row
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{:<w$}", cell_to_raw(c), w = widths[i]))
            .collect::<Vec<_>>()
            .join(" | ");
        println!("{line}");
    }
}

fn cell_to_raw(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(v) => v.to_string(),
        serde_json::Value::Number(v) => v.to_string(),
        serde_json::Value::String(v) => v.clone(),
        _ => value.to_string(),
    }
}

fn parse_range(value: &str) -> Option<(usize, usize)> {
    let v = value.strip_prefix("bytes=")?;
    let (a, b) = v.split_once('-')?;
    let start = a.parse::<usize>().ok()?;
    let end = if b.is_empty() {
        start
    } else {
        b.parse::<usize>().ok()?
    };
    if end < start {
        return None;
    }
    Some((start, end))
}

async fn stream_body_to_temp_file(body: Body) -> std::result::Result<std::path::PathBuf, Response> {
    let path = std::env::temp_dir().join(format!("stoar-http-upload-{}.bin", uuid::Uuid::new_v4()));
    let mut file = tokio::fs::File::create(&path).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("temp file create failed: {e}"),
        )
            .into_response()
    })?;

    let mut stream = body.into_data_stream();
    while let Some(next) = futures_util::StreamExt::next(&mut stream).await {
        let chunk = next.map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid request body: {e}"),
            )
                .into_response()
        })?;
        file.write_all(&chunk).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("temp file write failed: {e}"),
            )
                .into_response()
        })?;
    }
    file.flush().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("temp file flush failed: {e}"),
        )
            .into_response()
    })?;
    Ok(path)
}
