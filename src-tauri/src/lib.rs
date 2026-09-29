#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::Read,
    path::{Component, Path, PathBuf},
    process::{Command as StdCommand, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tauri::{AppHandle, Manager, State as TauriState};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    process::{Child, Command},
    sync::{broadcast, mpsc, oneshot, RwLock},
    time::timeout,
};
use tower_http::cors::CorsLayer;
use uuid::Uuid;

const MAX_PARTICIPANTS: usize = 10;
const RUN_TIMEOUT: Duration = Duration::from_secs(15);
const ROOM_ARCHIVE_FORMAT: &str = "code-with-me-room";
const ROOM_ARCHIVE_VERSION: u8 = 1;
const MAX_ARCHIVE_BYTES: usize = 100 * 1024 * 1024;
const MAX_ARCHIVE_JSON_BYTES: usize = 140 * 1024 * 1024;
const MAX_ARCHIVE_FILES: usize = 5000;
const MAX_CLOUDFLARED_DOWNLOAD_BYTES: usize = 80 * 1024 * 1024;
const CLOUDFLARED_RELEASE_API: &str =
    "https://api.github.com/repos/cloudflare/cloudflared/releases/latest";

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct RoomInfo {
    room_id: String,
    invite_code: String,
    invite_url: String,
    local_base: Option<String>,
    address: String,
    port: u16,
    participant_count: usize,
    max_participants: usize,
    participants: Vec<ParticipantView>,
}

#[derive(Clone, Serialize)]
struct ParticipantView {
    id: String,
    name: String,
    host: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FileEntry {
    name: String,
    path: String,
    is_directory: bool,
    extension: String,
    is_text: bool,
}

struct Participant {
    name: String,
    host: bool,
}

struct RoomData {
    info: RoomInfo,
    root: PathBuf,
    participants: RwLock<HashMap<String, Participant>>,
    events: broadcast::Sender<Value>,
}

struct RunningRoom {
    data: Arc<RoomData>,
    stop: oneshot::Sender<()>,
    tunnel: Child,
}

#[derive(Default)]
struct RoomManager {
    root: Mutex<Option<PathBuf>>,
    running: Mutex<Option<RunningRoom>>,
}

#[derive(Deserialize)]
struct RoomQuery {
    code: String,
}

#[derive(Deserialize)]
struct FileQuery {
    code: String,
    path: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NewEntry {
    code: String,
    path: String,
    kind: String,
    content: Option<String>,
    encoding: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RenameEntry {
    code: Option<String>,
    old_path: String,
    new_path: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunResult {
    success: bool,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    timed_out: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SocketQuery {
    code: String,
    name: Option<String>,
    host: Option<bool>,
}

#[derive(Deserialize)]
struct ExportQuery {
    code: String,
    mode: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RoomArchive {
    format: String,
    version: u8,
    mode: String,
    name: String,
    files: Vec<ArchiveFile>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArchiveFile {
    path: String,
    kind: String,
    encoding: String,
    content: String,
}

#[derive(Deserialize)]
struct CloudflaredRelease {
    tag_name: String,
    assets: Vec<CloudflaredAsset>,
}

#[derive(Deserialize)]
struct CloudflaredAsset {
    name: String,
    browser_download_url: String,
    digest: Option<String>,
}

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_notification::init())
        .manage(RoomManager::default())
        .setup(|app| {
            let root = app.path().app_data_dir()?.join("workspace");
            std::fs::create_dir_all(&root)?;
            seed_workspace(&root)?;
            *app.state::<RoomManager>().root.lock().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "Папка проекта недоступна")
            })? = Some(root);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            start_room,
            stop_room,
            current_room,
            set_project_root,
            project_root_name,
            list_project_files,
            read_project_file,
            save_project_file,
            create_project_entry,
            rename_project_entry,
            delete_project_entry,
            list_local_rooms,
            open_local_room,
            create_contest,
            check_dependencies,
            open_dependency_page,
            export_local_contest,
            delete_local_contest,
            run_local_file,
            run_local_source,
            export_room_archive,
            import_room_archive,
            create_local_room
        ])
        .run(tauri::generate_context!())
        .expect("Не удалось запустить Code with me");
}

#[tauri::command]
fn set_project_root(manager: TauriState<'_, RoomManager>, path: String) -> Result<String, String> {
    let root = PathBuf::from(path)
        .canonicalize()
        .map_err(|error| format!("Не удалось открыть папку: {error}"))?;
    if !root.is_dir() {
        return Err("Выбранный путь не является папкой".into());
    }
    *manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")? = Some(root.clone());
    Ok(root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("Проект")
        .to_string())
}

#[tauri::command]
fn project_root_name(manager: TauriState<'_, RoomManager>) -> Result<String, String> {
    let root = manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")?
        .clone();
    Ok(root
        .and_then(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "Проект".into()))
}

fn current_root(manager: &RoomManager) -> Result<PathBuf, String> {
    manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна".to_string())?
        .clone()
        .ok_or_else(|| "Папка проекта ещё не выбрана".to_string())
}

#[tauri::command]
fn export_room_archive(
    manager: TauriState<'_, RoomManager>,
    mode: String,
) -> Result<String, String> {
    let root = current_root(&manager)?;
    serialize_room_archive(&root, &mode)
}

#[tauri::command]
fn import_room_archive(
    app: AppHandle,
    manager: TauriState<'_, RoomManager>,
    archive: String,
) -> Result<String, String> {
    if archive.len() > MAX_ARCHIVE_JSON_BYTES {
        return Err("Файл комнаты слишком большой".into());
    }
    let archive: RoomArchive = serde_json::from_str(&archive)
        .map_err(|error| format!("Не удалось прочитать файл комнаты: {error}"))?;
    let root = unpack_room_archive(&app, archive, "imported")?;
    let name = root
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("Локальная комната")
        .to_string();
    *manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")? = Some(root);
    Ok(name)
}

#[tauri::command]
fn create_local_room(
    app: AppHandle,
    manager: TauriState<'_, RoomManager>,
    mode: String,
) -> Result<String, String> {
    let root = current_root(&manager)?;
    let serialized = serialize_room_archive(&root, &mode)?;
    let archive: RoomArchive = serde_json::from_str(&serialized)
        .map_err(|error| format!("Не удалось создать локальную комнату: {error}"))?;
    let local_root = unpack_room_archive(&app, archive, "saved")?;
    let name = local_root
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("Локальная комната")
        .to_string();
    *manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")? = Some(local_root);
    Ok(name)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalRoomEntry {
    name: String,
    folder: String,
    task_count: usize,
    category: String,
}

#[tauri::command]
fn list_local_rooms(app: AppHandle) -> Result<Vec<LocalRoomEntry>, String> {
    let app_data = app
        .path()
        .app_data_dir()
        .map_err(|error| error.to_string())?;
    let mut roots = vec![(
        "Моё рабочее пространство".to_string(),
        "workspace".to_string(),
        app_data.join("workspace"),
        "my".to_string(),
    )];
    let local_root = app_data.join("local-rooms");
    if let Ok(entries) = std::fs::read_dir(&local_root) {
        for entry in entries.flatten() {
            if !entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
                continue;
            }
            let folder = entry.file_name().to_string_lossy().into_owned();
            let root = entry.path();
            let (category, labeled_folder) = if let Some(name) = folder.strip_prefix("my-") {
                ("my", name)
            } else if let Some(name) = folder.strip_prefix("saved-") {
                ("saved", name)
            } else if let Some(name) = folder.strip_prefix("imported-") {
                ("saved", name)
            } else {
                ("saved", folder.as_str())
            };
            let display = labeled_folder
                .rsplit_once('-')
                .map(|(name, _)| name.replace('-', " "))
                .unwrap_or_else(|| labeled_folder.replace('-', " "));
            roots.push((display, folder, root, category.to_string()));
        }
    }
    let mut rooms = Vec::new();
    for (name, folder, root, category) in roots {
        let task_count = collect_files(&root)
            .map(|files| {
                files
                    .iter()
                    .filter(|file| !file.is_directory && file.name.ends_with(".cwm.md"))
                    .count()
            })
            .unwrap_or(0);
        rooms.push(LocalRoomEntry {
            name,
            folder,
            task_count,
            category,
        });
    }
    Ok(rooms)
}

#[tauri::command]
fn open_local_room(
    app: AppHandle,
    manager: TauriState<'_, RoomManager>,
    folder: String,
) -> Result<String, String> {
    let app_data = app
        .path()
        .app_data_dir()
        .map_err(|error| error.to_string())?;
    let root = if folder == "workspace" {
        app_data.join("workspace")
    } else {
        if folder.is_empty()
            || folder.contains('/')
            || folder.contains('\\')
            || folder == "."
            || folder == ".."
        {
            return Err("Недопустимый контест".into());
        }
        app_data.join("local-rooms").join(&folder)
    };
    let canonical = root
        .canonicalize()
        .map_err(|error| format!("Не удалось открыть контест: {error}"))?;
    if folder != "workspace" {
        let allowed_root = app_data
            .join("local-rooms")
            .canonicalize()
            .map_err(|error| format!("Не удалось открыть список контестов: {error}"))?;
        if !canonical.starts_with(&allowed_root) {
            return Err("Контест находится вне локального хранилища".into());
        }
    }
    if !canonical.is_dir() {
        return Err("Контест не найден".into());
    }
    *manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")? = Some(canonical.clone());
    Ok(canonical
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("Контест")
        .to_string())
}

fn local_contest_root(app: &AppHandle, folder: &str) -> Result<PathBuf, String> {
    let app_data = app
        .path()
        .app_data_dir()
        .map_err(|error| error.to_string())?;
    if folder.is_empty()
        || folder == "workspace"
        || folder.contains('/')
        || folder.contains('\\')
        || folder == "."
        || folder == ".."
    {
        return Err("Недопустимый сохранённый контест".into());
    }
    let base = app_data
        .join("local-rooms")
        .canonicalize()
        .map_err(|error| format!("Не удалось открыть хранилище контестов: {error}"))?;
    let root = base
        .join(folder)
        .canonicalize()
        .map_err(|error| format!("Контест не найден: {error}"))?;
    if !root.starts_with(&base) || !root.is_dir() {
        return Err("Контест находится вне локального хранилища".into());
    }
    Ok(root)
}

#[tauri::command]
fn export_local_contest(app: AppHandle, folder: String) -> Result<String, String> {
    let root = if folder == "workspace" {
        app.path()
            .app_data_dir()
            .map_err(|error| error.to_string())?
            .join("workspace")
    } else {
        local_contest_root(&app, &folder)?
    };
    serialize_room_archive(&root, "snapshot")
}

#[tauri::command]
fn delete_local_contest(
    app: AppHandle,
    manager: TauriState<'_, RoomManager>,
    folder: String,
) -> Result<(), String> {
    let root = local_contest_root(&app, &folder)?;
    {
        let running = manager
            .running
            .lock()
            .map_err(|_| "Состояние комнаты недоступно")?;
        if running.as_ref().is_some_and(|room| room.data.root == root) {
            return Err("Сначала остановите комнату, созданную из этого контеста".into());
        }
    }
    std::fs::remove_dir_all(&root)
        .map_err(|error| format!("Не удалось удалить контест: {error}"))?;
    let mut project_root = manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")?;
    if project_root.as_ref() == Some(&root) {
        let workspace = app
            .path()
            .app_data_dir()
            .map_err(|error| error.to_string())?
            .join("workspace");
        std::fs::create_dir_all(&workspace).map_err(|error| error.to_string())?;
        *project_root = Some(workspace);
    }
    Ok(())
}

#[tauri::command]
fn list_project_files(manager: TauriState<'_, RoomManager>) -> Result<Vec<FileEntry>, String> {
    let root = manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")?
        .clone()
        .ok_or_else(|| "Папка проекта ещё не выбрана".to_string())?;
    collect_files(&root).map_err(|error| error.to_string())
}

#[tauri::command]
fn read_project_file(manager: TauriState<'_, RoomManager>, path: String) -> Result<String, String> {
    let root = manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")?
        .clone()
        .ok_or_else(|| "Папка проекта ещё не выбрана".to_string())?;
    let file = safe_path(&root, &path).map_err(|_| "Недопустимый путь к файлу".to_string())?;
    if !is_text_file(&file) {
        return Err("Файл не является текстовым".into());
    }
    std::fs::read_to_string(file).map_err(|error| error.to_string())
}

#[tauri::command]
fn save_project_file(
    manager: TauriState<'_, RoomManager>,
    path: String,
    content: String,
) -> Result<(), String> {
    let root = manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")?
        .clone()
        .ok_or_else(|| "Папка проекта ещё не выбрана".to_string())?;
    let file = safe_path(&root, &path).map_err(|_| "Недопустимый путь к файлу".to_string())?;
    let parent = file
        .parent()
        .ok_or_else(|| "Путь к файлу некорректен".to_string())?;
    std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    std::fs::write(file, content).map_err(|error| error.to_string())
}

#[tauri::command]
fn create_project_entry(
    manager: TauriState<'_, RoomManager>,
    path: String,
    kind: String,
    content: Option<String>,
    encoding: Option<String>,
) -> Result<(), String> {
    let root = manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")?
        .clone()
        .ok_or_else(|| "Папка проекта ещё не выбрана".to_string())?;
    create_entry_at(
        &root,
        &path,
        &kind,
        content.unwrap_or_default(),
        encoding.as_deref(),
    )
    .map_err(|error| error.to_string())
}

fn create_entry_at(
    root: &Path,
    relative: &str,
    kind: &str,
    content: String,
    encoding: Option<&str>,
) -> std::io::Result<()> {
    let path = safe_path(root, relative)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Недопустимый путь"))?;
    if kind == "directory" {
        return std::fs::create_dir_all(path);
    }
    if kind != "file" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Неизвестный тип элемента",
        ));
    }
    if path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "Файл уже существует",
        ));
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = if encoding == Some("base64") {
        BASE64.decode(content).map_err(|error| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
        })?
    } else {
        content.into_bytes()
    };
    std::fs::write(path, bytes)
}

#[tauri::command]
fn rename_project_entry(
    manager: TauriState<'_, RoomManager>,
    old_path: String,
    new_path: String,
) -> Result<(), String> {
    let root = current_root(&manager)?;
    rename_entry_at(&root, &old_path, &new_path).map_err(|error| error.to_string())
}

#[tauri::command]
fn delete_project_entry(manager: TauriState<'_, RoomManager>, path: String) -> Result<(), String> {
    let root = current_root(&manager)?;
    delete_entry_at(&root, &path).map_err(|error| error.to_string())
}

fn delete_entry_at(root: &Path, relative: &str) -> std::io::Result<()> {
    let path = safe_path(root, relative)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Недопустимый путь"))?;
    let metadata = std::fs::symlink_metadata(&path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Можно удалять только файлы",
        ));
    }
    std::fs::remove_file(path)
}

fn rename_entry_at(root: &Path, old_path: &str, new_path: &str) -> std::io::Result<()> {
    let source = safe_path(root, old_path)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Недопустимый путь"))?;
    let destination = safe_path(root, new_path)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Недопустимый путь"))?;
    if destination.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "Решение с таким названием уже существует",
        ));
    }
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::rename(source, destination)
}

#[tauri::command]
async fn run_local_file(
    manager: TauriState<'_, RoomManager>,
    path: String,
) -> Result<RunResult, String> {
    let root = manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")?
        .clone()
        .ok_or_else(|| "Папка проекта ещё не выбрана".to_string())?;
    let file = safe_path(&root, &path).map_err(|_| "Недопустимый путь к файлу".to_string())?;
    execute_project_file(&root, &file).await
}

#[tauri::command]
async fn run_local_source(extension: String, source: String) -> Result<RunResult, String> {
    let extension = extension.to_lowercase();
    if !matches!(
        extension.as_str(),
        "py" | "go" | "cpp" | "cc" | "cxx" | "java"
    ) {
        return Err("Поддерживаются Python, Go, C++ и Java".into());
    }
    if source.len() > 5 * 1024 * 1024 {
        return Err("Исходный файл больше 5 МБ".into());
    }
    let root = std::env::temp_dir()
        .join("code-with-me-runs")
        .join(Uuid::new_v4().simple().to_string());
    tokio::fs::create_dir_all(&root)
        .await
        .map_err(|error| error.to_string())?;
    let source_path = root.join(format!("solution.{extension}"));
    if let Err(error) = tokio::fs::write(&source_path, source).await {
        let _ = tokio::fs::remove_dir_all(&root).await;
        return Err(format!("Не удалось подготовить запуск: {error}"));
    }
    let result = execute_isolated_file(&root, &source_path).await;
    let _ = tokio::fs::remove_dir_all(&root).await;
    result
}

fn seed_workspace(root: &Path) -> std::io::Result<()> {
    let solution = root.join("0.solution-0.py");
    if !solution.exists() {
        let legacy_solution = root.join("main.py");
        if legacy_solution.exists() {
            std::fs::copy(legacy_solution, &solution)?;
        } else {
            std::fs::write(&solution, include_str!("../../workspace/0.solution-0.py"))?;
        }
    }

    let task = root.join("task.cwm.md");
    if !task.exists() {
        std::fs::write(&task, include_str!("../../task.cwm.md"))?;
    } else {
        let source = std::fs::read_to_string(&task)?;
        if source.contains("entrypoint: main.py") {
            std::fs::write(
                &task,
                source.replace("entrypoint: main.py", "entrypoint: 0.solution-0.py"),
            )?;
        }
    }
    Ok(())
}

fn is_code_file(path: &Path) -> bool {
    matches!(
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("")
            .to_lowercase()
            .as_str(),
        "py" | "go" | "cpp" | "cc" | "cxx" | "h" | "hpp" | "java"
    )
}

fn archive_mode(mode: &str) -> Result<&str, String> {
    match mode {
        "template" | "snapshot" => Ok(mode),
        _ => Err("Неизвестный вариант сохранения комнаты".into()),
    }
}

fn serialize_room_archive(root: &Path, mode: &str) -> Result<String, String> {
    let mode = archive_mode(mode)?;
    let entries = collect_files(root).map_err(|error| error.to_string())?;
    if entries.len() > MAX_ARCHIVE_FILES {
        return Err("В комнате слишком много файлов для сохранения".into());
    }
    let mut total_size = 0usize;
    let mut files = Vec::new();
    for entry in entries {
        let path = safe_path(root, &entry.path)
            .map_err(|_| "В проекте найден недопустимый путь".to_string())?;
        if entry.is_directory {
            files.push(ArchiveFile {
                path: entry.path,
                kind: "directory".into(),
                encoding: "none".into(),
                content: String::new(),
            });
            continue;
        }
        let mut bytes = std::fs::read(&path).map_err(|error| error.to_string())?;
        if mode == "template" && is_code_file(&path) {
            bytes.clear();
        }
        total_size = total_size.saturating_add(bytes.len());
        if total_size > MAX_ARCHIVE_BYTES {
            return Err("Комната больше 100 МБ".into());
        }
        let (encoding, content) = match String::from_utf8(bytes.clone()) {
            Ok(text) => ("utf8".to_string(), text),
            Err(_) => ("base64".to_string(), BASE64.encode(bytes)),
        };
        files.push(ArchiveFile {
            path: entry.path,
            kind: "file".into(),
            encoding,
            content,
        });
    }
    let name = root
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("Занятие")
        .to_string();
    serde_json::to_string_pretty(&RoomArchive {
        format: ROOM_ARCHIVE_FORMAT.into(),
        version: ROOM_ARCHIVE_VERSION,
        mode: mode.into(),
        name,
        files,
    })
    .map_err(|error| error.to_string())
}

fn unpack_room_archive(
    app: &AppHandle,
    archive: RoomArchive,
    category: &str,
) -> Result<PathBuf, String> {
    if archive.format != ROOM_ARCHIVE_FORMAT || archive.version != ROOM_ARCHIVE_VERSION {
        return Err("Версия файла комнаты не поддерживается".into());
    }
    archive_mode(&archive.mode)?;
    if archive.files.len() > MAX_ARCHIVE_FILES {
        return Err("В файле комнаты слишком много файлов".into());
    }
    let safe_name: String = archive
        .name
        .chars()
        .map(|character| {
            if character.is_alphanumeric() || matches!(character, '-' | '_' | ' ') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim()
        .chars()
        .take(48)
        .collect();
    let suffix = &Uuid::new_v4().simple().to_string()[..8];
    let folder_name = format!(
        "{category}-{}-{suffix}",
        if safe_name.is_empty() {
            "local-room"
        } else {
            safe_name.as_str()
        }
    );
    let root = app
        .path()
        .app_data_dir()
        .map_err(|error| error.to_string())?
        .join("local-rooms")
        .join(folder_name);
    let mut total_size = 0usize;
    let mut decoded_entries = Vec::with_capacity(archive.files.len());
    for file in archive.files {
        let destination = safe_path(&root, &file.path)
            .map_err(|_| format!("Недопустимый путь в комнате: {}", file.path))?;
        if file.kind == "directory" {
            decoded_entries.push((destination, None));
            continue;
        }
        if file.kind != "file" {
            return Err(format!("Неизвестный тип элемента: {}", file.path));
        }
        let bytes = match file.encoding.as_str() {
            "utf8" => file.content.into_bytes(),
            "base64" => BASE64
                .decode(file.content)
                .map_err(|error| format!("Повреждён файл {}: {error}", file.path))?,
            _ => return Err(format!("Неизвестная кодировка файла {}", file.path)),
        };
        total_size = total_size.saturating_add(bytes.len());
        if total_size > MAX_ARCHIVE_BYTES {
            return Err("Файл комнаты больше 100 МБ".into());
        }
        decoded_entries.push((destination, Some(bytes)));
    }
    std::fs::create_dir_all(&root).map_err(|error| error.to_string())?;
    for (destination, bytes) in decoded_entries {
        if bytes.is_none() {
            std::fs::create_dir_all(destination).map_err(|error| error.to_string())?;
            continue;
        }
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        std::fs::write(destination, bytes.unwrap()).map_err(|error| error.to_string())?;
    }
    Ok(root)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DependencyStatus {
    id: String,
    name: String,
    description: String,
    installed: bool,
    version: String,
}

#[tauri::command]
fn check_dependencies() -> Vec<DependencyStatus> {
    let specs = [
        (
            "python",
            "Python",
            "Для запуска решений Python",
            if cfg!(windows) {
                vec!["python", "python3"]
            } else {
                vec!["python3", "python"]
            },
        ),
        ("go", "Go", "Для сборки и запуска решений Go", vec!["go"]),
        (
            "cpp",
            "Компилятор C++",
            "Нужен C++20; приложение ищет g++ в Windows и c++ в macOS/Linux",
            if cfg!(windows) {
                vec!["g++"]
            } else {
                vec!["c++", "g++", "clang++"]
            },
        ),
        (
            "java",
            "Java JDK",
            "Для компиляции и запуска решений Java",
            vec!["javac"],
        ),
    ];
    specs
        .into_iter()
        .map(|(id, name, description, candidates)| {
            let found = candidates.into_iter().find_map(find_executable);
            let checked = found
                .as_ref()
                .and_then(|path| {
                    let args: &[&str] = if id == "java" {
                        &["-version"]
                    } else if id == "go" {
                        &["version"]
                    } else {
                        &["--version"]
                    };
                    StdCommand::new(path)
                        .args(args)
                        .output()
                        .ok()
                        .filter(|output| output.status.success())
                        .map(|output| {
                            let text = if output.stdout.is_empty() {
                                output.stderr
                            } else {
                                output.stdout
                            };
                            String::from_utf8_lossy(&text)
                                .lines()
                                .next()
                                .unwrap_or("")
                                .trim()
                                .to_string()
                        })
                })
                .unwrap_or_default();
            let installed = found.is_some() && !checked.is_empty();
            DependencyStatus {
                id: id.into(),
                name: name.into(),
                description: description.into(),
                installed,
                version: checked,
            }
        })
        .collect()
}

fn find_executable(command: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(command);
        if candidate.is_file() {
            return Some(candidate);
        }
        #[cfg(windows)]
        for extension in ["exe", "cmd", "bat"] {
            let candidate = directory.join(format!("{command}.{extension}"));
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

#[tauri::command]
fn open_dependency_page(id: String) -> Result<(), String> {
    let url = match id.as_str() {
        "python" => "https://www.python.org/downloads/",
        "go" => "https://go.dev/dl/",
        "cpp" if cfg!(target_os = "macos") => "https://developer.apple.com/xcode/resources/",
        "cpp" if cfg!(target_os = "windows") => "https://www.msys2.org/",
        "cpp" => "https://clang.llvm.org/get_started.html",
        "java" => "https://adoptium.net/temurin/releases/",
        _ => return Err("Неизвестная зависимость".into()),
    };
    #[cfg(target_os = "macos")]
    let result = StdCommand::new("open").arg(url).spawn();
    #[cfg(target_os = "windows")]
    let result = StdCommand::new("cmd")
        .args(["/C", "start", "", url])
        .spawn();
    #[cfg(target_os = "linux")]
    let result = StdCommand::new("xdg-open").arg(url).spawn();
    result
        .map(|_| ())
        .map_err(|error| format!("Не удалось открыть страницу установки: {error}"))
}

#[tauri::command]
fn create_contest(
    app: AppHandle,
    manager: TauriState<'_, RoomManager>,
    name: String,
) -> Result<String, String> {
    let name: String = name
        .trim()
        .chars()
        .filter(|character| !character.is_control() && !matches!(character, '\r' | '\n' | ':'))
        .take(60)
        .collect();
    if name.trim().is_empty() {
        return Err("Введите название контеста".into());
    }
    let task = format!("---\ntype: challenge\ntitle: {}\ntheme: Практика\nsubtopic: \ncategory: Практика\ndifficulty: medium\ntime_limit: 20\nlanguage: python\nentrypoint: 0.solution-0.py\n---\n\nОпишите условие задания.\n", name.trim());
    let archive = RoomArchive {
        format: ROOM_ARCHIVE_FORMAT.into(),
        version: ROOM_ARCHIVE_VERSION,
        mode: "snapshot".into(),
        name: name.trim().into(),
        files: vec![
            ArchiveFile {
                path: "task.cwm.md".into(),
                kind: "file".into(),
                encoding: "utf8".into(),
                content: task,
            },
            ArchiveFile {
                path: "0.solution-0.py".into(),
                kind: "file".into(),
                encoding: "utf8".into(),
                content: "".into(),
            },
        ],
    };
    let root = unpack_room_archive(&app, archive, "my")?;
    *manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")? = Some(root.clone());
    Ok(root
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("Контест")
        .to_string())
}

#[tauri::command]
async fn start_room(
    app: AppHandle,
    manager: TauriState<'_, RoomManager>,
) -> Result<RoomInfo, String> {
    let already_running = {
        manager
            .running
            .lock()
            .map_err(|_| "Состояние комнаты недоступно")?
            .as_ref()
            .map(|existing| existing.data.clone())
    };
    if let Some(existing) = already_running {
        return Ok(room_info(&existing).await);
    }
    let root = manager
        .root
        .lock()
        .map_err(|_| "Папка проекта недоступна")?
        .clone()
        .ok_or_else(|| "Папка проекта ещё не настроена".to_string())?;
    let cloudflared = ensure_cloudflared(&app).await?;
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .map_err(|error| format!("Не удалось запустить комнату: {error}"))?;
    let port = listener
        .local_addr()
        .map_err(|error| error.to_string())?
        .port();
    let (public_base, tunnel) = start_public_tunnel(&cloudflared, port).await?;
    let address = public_base
        .strip_prefix("https://")
        .unwrap_or(&public_base)
        .to_string();
    let invite_code = Uuid::new_v4().simple().to_string();
    let room_id = Uuid::new_v4().to_string();
    let host_id = Uuid::new_v4().to_string();
    let (events, _) = broadcast::channel(256);
    let mut participants = HashMap::new();
    participants.insert(
        host_id.clone(),
        Participant {
            name: "Хозяин комнаты".into(),
            host: true,
        },
    );
    let url = format!("{public_base}/?code={invite_code}");
    let info = RoomInfo {
        room_id,
        invite_code: invite_code.clone(),
        invite_url: url,
        local_base: Some(format!("http://127.0.0.1:{port}")),
        address,
        port: 443,
        participant_count: 1,
        max_participants: MAX_PARTICIPANTS,
        participants: vec![ParticipantView {
            id: host_id,
            name: "Хозяин комнаты".into(),
            host: true,
        }],
    };
    let data = Arc::new(RoomData {
        info,
        root,
        participants: RwLock::new(participants),
        events,
    });
    let router = room_router(data.clone());
    let (stop, stop_rx) = oneshot::channel();
    tauri::async_runtime::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stop_rx.await;
            })
            .await;
    });
    let room = RunningRoom {
        data: data.clone(),
        stop,
        tunnel,
    };
    *manager
        .running
        .lock()
        .map_err(|_| "Состояние комнаты недоступно")? = Some(room);
    Ok(room_info(&data).await)
}

#[tauri::command]
async fn stop_room(manager: TauriState<'_, RoomManager>) -> Result<(), String> {
    let room = {
        let mut running = manager
            .running
            .lock()
            .map_err(|_| "Состояние комнаты недоступно")?;
        running.take()
    };
    if let Some(mut room) = room {
        let _ = room.stop.send(());
        let _ = room.tunnel.kill().await;
    }
    Ok(())
}

#[tauri::command]
async fn current_room(manager: TauriState<'_, RoomManager>) -> Result<Option<RoomInfo>, String> {
    let room = manager
        .running
        .lock()
        .map_err(|_| "Состояние комнаты недоступно")?
        .as_ref()
        .map(|room| room.data.clone());
    match room {
        Some(data) => Ok(Some(room_info(&data).await)),
        None => Ok(None),
    }
}

async fn room_info(room: &RoomData) -> RoomInfo {
    let participants = room.participants.read().await;
    let mut list: Vec<_> = participants
        .iter()
        .map(|(id, p)| ParticipantView {
            id: id.clone(),
            name: p.name.clone(),
            host: p.host,
        })
        .collect();
    list.sort_by(|a, b| b.host.cmp(&a.host).then(a.name.cmp(&b.name)));
    let mut info = room.info.clone();
    info.participant_count = list.len();
    info.participants = list;
    info
}

async fn ensure_cloudflared(app: &AppHandle) -> Result<PathBuf, String> {
    let (asset_name, archive) = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "x86_64") => ("cloudflared-darwin-amd64.tgz", true),
        ("macos", "aarch64") => ("cloudflared-darwin-arm64.tgz", true),
        ("linux", "x86_64") => ("cloudflared-linux-amd64", false),
        ("linux", "aarch64") => ("cloudflared-linux-arm64", false),
        ("windows", "x86_64") => ("cloudflared-windows-amd64.exe", false),
        (os, arch) => {
            return Err(format!(
                "Публичные комнаты пока не поддерживаются на этой архитектуре ({os}/{arch})"
            ));
        }
    };
    let binary_name = if std::env::consts::OS == "windows" {
        "cloudflared.exe"
    } else {
        "cloudflared"
    };
    let tool_dir = app
        .path()
        .app_data_dir()
        .map_err(|error| format!("Не удалось найти папку данных приложения: {error}"))?
        .join("tools")
        .join(format!(
            "cloudflared-{}-{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        ));
    std::fs::create_dir_all(&tool_dir)
        .map_err(|error| format!("Не удалось подготовить публичный доступ: {error}"))?;
    let installed_binary = tool_dir.join(binary_name);
    let cached_binary = std::fs::metadata(&installed_binary)
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 1_000_000);
    let cached_version = std::fs::read_to_string(tool_dir.join("version")).unwrap_or_default();

    let client = reqwest::Client::builder()
        .user_agent("Code-with-me-desktop")
        .timeout(Duration::from_secs(45))
        .build()
        .map_err(|error| format!("Не удалось подготовить загрузчик туннеля: {error}"))?;
    let release_response = client
        .get(CLOUDFLARED_RELEASE_API)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|error| error.to_string());
    let release_response = match release_response {
        Ok(response) => response,
        Err(_) if cached_binary => return Ok(installed_binary),
        Err(error) => {
            return Err(format!(
                "Для публичной комнаты нужно загрузить сетевой компонент. Проверьте подключение к интернету и повторите попытку: {error}"
            ));
        }
    };
    let release: CloudflaredRelease = match release_response.json().await {
        Ok(release) => release,
        Err(_) if cached_binary => return Ok(installed_binary),
        Err(error) => {
            return Err(format!(
                "Не удалось прочитать выпуск сетевого компонента: {error}"
            ));
        }
    };
    let release_tag = release.tag_name;
    if release_tag.is_empty()
        || !release_tag
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || ".-_".contains(character))
    {
        return Err("Получено некорректное имя выпуска сетевого компонента".into());
    }
    if cached_binary && cached_version.trim() == release_tag {
        return Ok(installed_binary);
    }
    let asset = release
        .assets
        .into_iter()
        .find(|asset| asset.name == asset_name)
        .ok_or_else(|| format!("В выпуске cloudflared нет файла {asset_name}"))?;
    if !asset
        .browser_download_url
        .starts_with("https://github.com/cloudflare/cloudflared/releases/download/")
    {
        return Err("Источник сетевого компонента не прошёл проверку".into());
    }
    let expected_digest = asset
        .digest
        .as_deref()
        .and_then(|value| value.strip_prefix("sha256:"))
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| "Выпуск cloudflared не содержит контрольную сумму SHA-256".to_string())?;
    let response = client
        .get(&asset.browser_download_url)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|error| format!("Не удалось загрузить сетевой компонент: {error}"))?;
    if response
        .content_length()
        .is_some_and(|size| size > MAX_CLOUDFLARED_DOWNLOAD_BYTES as u64)
    {
        return Err("Файл сетевого компонента превышает допустимый размер".into());
    }
    let package = response
        .bytes()
        .await
        .map_err(|error| format!("Не удалось получить сетевой компонент: {error}"))?;
    if package.len() > MAX_CLOUDFLARED_DOWNLOAD_BYTES {
        return Err("Файл сетевого компонента превышает допустимый размер".into());
    }
    let actual_digest = format!("{:x}", Sha256::digest(&package));
    if !actual_digest.eq_ignore_ascii_case(expected_digest) {
        return Err("Контрольная сумма сетевого компонента не совпала".into());
    }
    let binary = if archive {
        extract_cloudflared_binary(&package)?
    } else {
        package.to_vec()
    };
    if binary.len() < 1_000_000 {
        return Err("Загруженный сетевой компонент выглядит повреждённым".into());
    }
    let pending = tool_dir.join(format!("{binary_name}.download"));
    std::fs::write(&pending, binary)
        .map_err(|error| format!("Не удалось сохранить сетевой компонент: {error}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pending, std::fs::Permissions::from_mode(0o755))
            .map_err(|error| format!("Не удалось разрешить запуск сетевого компонента: {error}"))?;
    }
    if installed_binary.exists() {
        std::fs::remove_file(&installed_binary)
            .map_err(|error| format!("Не удалось обновить сетевой компонент: {error}"))?;
    }
    std::fs::rename(&pending, &installed_binary)
        .map_err(|error| format!("Не удалось установить сетевой компонент: {error}"))?;
    std::fs::write(tool_dir.join("version"), release_tag)
        .map_err(|error| format!("Не удалось сохранить версию сетевого компонента: {error}"))?;
    Ok(installed_binary)
}

fn extract_cloudflared_binary(package: &[u8]) -> Result<Vec<u8>, String> {
    let decoder = flate2::read::GzDecoder::new(package);
    let mut expanded = Vec::new();
    decoder
        .take(MAX_CLOUDFLARED_DOWNLOAD_BYTES as u64)
        .read_to_end(&mut expanded)
        .map_err(|error| format!("Не удалось распаковать сетевой компонент: {error}"))?;
    if expanded.len() >= MAX_CLOUDFLARED_DOWNLOAD_BYTES {
        return Err("Распакованный сетевой компонент превышает допустимый размер".into());
    }
    let mut offset = 0;
    while offset + 512 <= expanded.len() {
        let header = &expanded[offset..offset + 512];
        if header.iter().all(|byte| *byte == 0) {
            break;
        }
        let name = std::str::from_utf8(&header[..100])
            .unwrap_or("")
            .trim_matches('\0');
        let prefix = std::str::from_utf8(&header[345..500])
            .unwrap_or("")
            .trim_matches('\0');
        let relative = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}/{name}")
        };
        let size_field = std::str::from_utf8(&header[124..136])
            .unwrap_or("")
            .trim_matches('\0')
            .trim();
        let size = usize::from_str_radix(size_field, 8)
            .map_err(|_| "Архив сетевого компонента повреждён".to_string())?;
        let payload_start = offset + 512;
        let payload_end = payload_start.saturating_add(size);
        if payload_end > expanded.len() {
            return Err("Архив сетевого компонента обрезан".into());
        }
        if Path::new(&relative)
            .file_name()
            .and_then(|value| value.to_str())
            == Some("cloudflared")
            && matches!(header[156], 0 | b'0')
        {
            return Ok(expanded[payload_start..payload_end].to_vec());
        }
        offset = payload_start + size.div_ceil(512) * 512;
    }
    Err("В архиве не найден исполняемый файл cloudflared".into())
}

async fn forward_tunnel_output<R>(stream: R, sender: mpsc::UnboundedSender<String>)
where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let shortened: String = line.chars().take(2000).collect();
        // Keep draining cloudflared's pipes after the invite URL has been
        // captured. Closing them can make the tunnel process exit or block.
        let _ = sender.send(shortened);
    }
}

fn append_tunnel_log(log_lines: &mut Vec<String>, line: String) {
    const MAX_TUNNEL_LOG_LINES: usize = 32;
    if log_lines.len() == MAX_TUNNEL_LOG_LINES {
        log_lines.remove(0);
    }
    log_lines.push(line);
}

fn tunnel_precheck_hint(log_lines: &[String]) -> Option<&'static str> {
    let failed_checks: Vec<_> = log_lines
        .iter()
        .filter(|line| line.contains("precheck component=") && line.contains("status=fail"))
        .collect();

    if failed_checks
        .iter()
        .any(|line| line.contains("DNS Resolution"))
    {
        return Some(
            "Не удалось разрешить адреса Cloudflare. Проверьте настройки DNS и подключение VPN или прокси.",
        );
    }

    let udp_failed = failed_checks
        .iter()
        .any(|line| line.contains("UDP Connectivity"));
    let tcp_failed = failed_checks
        .iter()
        .any(|line| line.contains("TCP Connectivity"));
    if udp_failed && tcp_failed {
        return Some(
            "Сеть не пропускает соединение к Cloudflare через UDP или TCP на порту 7844. Проверьте настройки сети или брандмауэра.",
        );
    }

    log_lines
        .iter()
        .any(|line| line.contains("precheck complete hard_fail=true"))
        .then_some(
            "Проверка соединения с Cloudflare не пройдена. Проверьте DNS, VPN или прокси и доступ к Интернету.",
        )
}

fn tunnel_url_from_log(line: &str) -> Option<String> {
    let start = line.find("https://")?;
    let candidate = line[start..]
        .split_whitespace()
        .next()?
        .trim_end_matches(['.', ',', ')', ']', '}', '"', '\'']);
    let host = candidate.strip_prefix("https://")?.split('/').next()?;
    (host.ends_with(".trycloudflare.com") && host.len() > ".trycloudflare.com".len())
        .then(|| format!("https://{host}"))
}

async fn start_public_tunnel(executable: &Path, port: u16) -> Result<(String, Child), String> {
    let origin = format!("http://127.0.0.1:{port}");
    let mut command = Command::new(executable);
    command
        .arg("tunnel")
        .arg("--no-autoupdate")
        .arg("--protocol")
        .arg("http2")
        .arg("--url")
        .arg(origin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.as_std_mut().creation_flags(0x08000000);
    }
    let mut child = command
        .spawn()
        .map_err(|error| format!("Не удалось запустить публичный туннель: {error}"))?;
    let (sender, mut output) = mpsc::unbounded_channel();
    if let Some(stdout) = child.stdout.take() {
        tauri::async_runtime::spawn(forward_tunnel_output(stdout, sender.clone()));
    }
    if let Some(stderr) = child.stderr.take() {
        tauri::async_runtime::spawn(forward_tunnel_output(stderr, sender.clone()));
    }
    drop(sender);

    let mut log_lines = Vec::new();
    let mut precheck_lines = Vec::new();
    let mut public_url = None;
    let mut connection_registered = false;
    let tunnel = timeout(Duration::from_secs(90), async {
        loop {
            tokio::select! {
                line = output.recv() => match line {
                    Some(line) => {
                        if let Some(url) = tunnel_url_from_log(&line) {
                            public_url = Some(url);
                        }
                        if line.contains("Registered tunnel connection") {
                            connection_registered = true;
                        }
                        if connection_registered {
                            if let Some(url) = public_url.take() {
                                return Ok(url);
                            }
                        }
                        if line.contains("precheck component=")
                            || line.contains("precheck complete")
                        {
                            append_tunnel_log(&mut precheck_lines, line.clone());
                        }
                        append_tunnel_log(&mut log_lines, line);
                    }
                    None => return Err("Публичный туннель закрыл журнал запуска".to_string()),
                },
                status = child.wait() => {
                    let detail = status
                        .map(|status| format!("Код завершения: {status}"))
                        .unwrap_or_else(|error| error.to_string());
                    // stdout and stderr readers can still have buffered lines after the
                    // process exits. Drain them before building the diagnostic message.
                    let _ = timeout(Duration::from_secs(2), async {
                        while let Some(line) = output.recv().await {
                            if line.contains("precheck component=")
                                || line.contains("precheck complete")
                            {
                                append_tunnel_log(&mut precheck_lines, line.clone());
                            }
                            append_tunnel_log(&mut log_lines, line);
                        }
                    }).await;
                    let hint = tunnel_precheck_hint(&precheck_lines);
                    let details = log_lines.join(" ");
                    let message = if let Some(hint) = hint {
                        format!("{hint} {detail}. {details}")
                    } else if details.is_empty() {
                        format!("Публичный туннель завершился до создания ссылки. {detail}")
                    } else {
                        format!("Публичный туннель завершился до создания ссылки. {detail}. {details}")
                    };
                    return Err(message);
                }
            }
        }
    })
    .await;
    let public_base = match tunnel {
        Ok(Ok(url)) => url,
        Ok(Err(error)) => {
            let _ = child.kill().await;
            let details = log_lines.join(" ");
            return Err(if details.is_empty() {
                error
            } else {
                format!("{error}. {details}")
            });
        }
        Err(_) => {
            let _ = child.kill().await;
            let details = log_lines.join(" ");
            return Err(if details.is_empty() {
                "Публичный туннель не подключился к Cloudflare за 90 секунд. Проверьте соединение и повторите попытку".into()
            } else {
                format!("Публичный туннель не подключился к Cloudflare за 90 секунд. {details}")
            });
        }
    };
    if child
        .try_wait()
        .map_err(|error| format!("Не удалось проверить состояние туннеля: {error}"))?
        .is_some()
    {
        return Err("Публичный туннель завершился сразу после запуска".into());
    }
    Ok((public_base, child))
}

fn room_router(room: Arc<RoomData>) -> Router {
    Router::new()
        .route("/", get(home))
        .route("/index.html", get(home))
        .route("/app.js", get(app_js))
        .route("/styles.css", get(styles_css))
        .route("/collapsed.css", get(collapsed_css))
        .route("/lesson.css", get(lesson_css))
        .route("/contest.css", get(contest_css))
        .route("/studio.css", get(studio_css))
        .route("/task.cwm.md", get(task_file))
        .route("/api/room", get(get_room))
        .route("/api/files", get(list_files).post(create_entry))
        .route(
            "/api/file",
            get(read_file).put(write_file).delete(delete_file),
        )
        .route("/api/rename", post(rename_entry))
        .route("/api/export", get(export_room))
        .route("/ws", get(websocket))
        .layer(CorsLayer::permissive())
        .with_state(room)
}

async fn home() -> Html<&'static str> {
    Html(include_str!("../../index.html"))
}
async fn app_js() -> impl IntoResponse {
    static_asset(
        "text/javascript; charset=utf-8",
        include_str!("../../app.js"),
    )
}
async fn styles_css() -> impl IntoResponse {
    static_asset("text/css; charset=utf-8", include_str!("../../styles.css"))
}
async fn collapsed_css() -> impl IntoResponse {
    static_asset(
        "text/css; charset=utf-8",
        include_str!("../../collapsed.css"),
    )
}
async fn lesson_css() -> impl IntoResponse {
    static_asset("text/css; charset=utf-8", include_str!("../../lesson.css"))
}
async fn contest_css() -> impl IntoResponse {
    static_asset("text/css; charset=utf-8", include_str!("../../contest.css"))
}
async fn studio_css() -> impl IntoResponse {
    static_asset("text/css; charset=utf-8", include_str!("../../studio.css"))
}
async fn task_file() -> impl IntoResponse {
    static_asset(
        "text/markdown; charset=utf-8",
        include_str!("../../task.cwm.md"),
    )
}

fn static_asset(content_type: &'static str, contents: &'static str) -> Response {
    ([(header::CONTENT_TYPE, content_type)], contents).into_response()
}

async fn get_room(State(room): State<Arc<RoomData>>, Query(query): Query<RoomQuery>) -> Response {
    if query.code != room.info.invite_code {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    Json(room_info(&room).await).into_response()
}

async fn export_room(
    State(room): State<Arc<RoomData>>,
    Query(query): Query<ExportQuery>,
) -> Response {
    if query.code != room.info.invite_code {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match serialize_room_archive(&room.root, &query.mode) {
        Ok(archive) => (
            [
                (
                    header::CONTENT_TYPE,
                    "application/vnd.code-with-me.room+json",
                ),
                (
                    header::CONTENT_DISPOSITION,
                    if query.mode == "template" {
                        "attachment; filename=code-with-me-template.cwmroom"
                    } else {
                        "attachment; filename=code-with-me-snapshot.cwmroom"
                    },
                ),
                (header::CACHE_CONTROL, "no-store"),
            ],
            archive,
        )
            .into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, error).into_response(),
    }
}

async fn list_files(State(room): State<Arc<RoomData>>, Query(query): Query<RoomQuery>) -> Response {
    if query.code != room.info.invite_code {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match collect_files(&room.root) {
        Ok(entries) => Json(entries).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

fn collect_files(root: &Path) -> std::io::Result<Vec<FileEntry>> {
    fn walk(root: &Path, folder: &Path, found: &mut Vec<FileEntry>) -> std::io::Result<()> {
        for item in std::fs::read_dir(folder)? {
            let item = item?;
            let kind = item.file_type()?;
            if kind.is_symlink() {
                continue;
            }
            let path = item.path();
            let relative = path.strip_prefix(root).unwrap_or(&path);
            let relative_text = relative.to_string_lossy().replace('\\', "/");
            if relative_text.split('/').any(|part| {
                matches!(
                    part,
                    ".git" | "node_modules" | ".cwm-build" | ".cwm-build-java" | ".cwm-runs"
                )
            }) {
                continue;
            }
            let is_directory = kind.is_dir();
            found.push(FileEntry {
                name: item.file_name().to_string_lossy().into_owned(),
                path: relative_text,
                is_directory,
                extension: path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .unwrap_or("")
                    .to_lowercase(),
                is_text: is_text_file(&path),
            });
            if is_directory {
                walk(root, &path, found)?;
            }
        }
        Ok(())
    }
    let mut entries = Vec::new();
    walk(root, root, &mut entries)?;
    entries.sort_by(|a, b| a.path.to_lowercase().cmp(&b.path.to_lowercase()));
    Ok(entries)
}

fn is_text_file(path: &Path) -> bool {
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_lowercase();
    matches!(
        ext.as_str(),
        "py" | "go"
            | "cpp"
            | "cc"
            | "cxx"
            | "h"
            | "hpp"
            | "java"
            | "txt"
            | "md"
            | "cwm"
            | "json"
            | "toml"
            | "yaml"
            | "yml"
            | "xml"
            | "html"
            | "css"
            | "js"
            | "ts"
            | "csv"
            | "log"
            | "rs"
            | "sh"
            | "sql"
    )
}

async fn read_file(State(room): State<Arc<RoomData>>, Query(query): Query<FileQuery>) -> Response {
    if query.code != room.info.invite_code {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let path = match safe_path(&room.root, &query.path) {
        Ok(path) => path,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    if !is_text_file(&path) {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    }
    match tokio::fs::read_to_string(path).await {
        Ok(content) => Json(json!({"content": content})).into_response(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            StatusCode::NOT_FOUND.into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn write_file(
    State(room): State<Arc<RoomData>>,
    Query(query): Query<FileQuery>,
    body: String,
) -> Response {
    if query.code != room.info.invite_code {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let path = match safe_path(&room.root, &query.path) {
        Ok(path) => path,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let parent = match path.parent() {
        Some(parent) => parent,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };
    if let Err(error) = tokio::fs::create_dir_all(parent).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
    }
    if let Err(error) = tokio::fs::write(path, &body).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
    }
    let _ = room
        .events
        .send(json!({"type":"file:saved", "path":query.path, "content":body}));
    StatusCode::NO_CONTENT.into_response()
}

async fn delete_file(
    State(room): State<Arc<RoomData>>,
    Query(query): Query<FileQuery>,
) -> Response {
    if query.code != room.info.invite_code {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match delete_entry_at(&room.root, &query.path) {
        Ok(()) => {
            let _ = room
                .events
                .send(json!({"type":"file:deleted", "path":query.path}));
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            StatusCode::NOT_FOUND.into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

async fn create_entry(State(room): State<Arc<RoomData>>, Json(entry): Json<NewEntry>) -> Response {
    if entry.code != room.info.invite_code {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let path = match safe_path(&room.root, &entry.path) {
        Ok(path) => path,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let result = match entry.kind.as_str() {
        "directory" => tokio::fs::create_dir_all(&path).await,
        "file" => {
            async {
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                if tokio::fs::try_exists(&path).await? {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        "Файл уже существует",
                    ));
                }
                let content = entry.content.unwrap_or_default();
                let bytes = if entry.encoding.as_deref() == Some("base64") {
                    BASE64.decode(content).map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                    })?
                } else {
                    content.into_bytes()
                };
                tokio::fs::write(&path, bytes).await
            }
            .await
        }
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    match result {
        Ok(()) => {
            let _ = room
                .events
                .send(json!({"type":"file:created", "path":entry.path, "kind":entry.kind}));
            (StatusCode::CREATED, Json(json!({"created":true}))).into_response()
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            (StatusCode::CONFLICT, error.to_string()).into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn rename_entry(
    State(room): State<Arc<RoomData>>,
    Json(entry): Json<RenameEntry>,
) -> Response {
    if entry.code.as_deref() != Some(room.info.invite_code.as_str()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match rename_entry_at(&room.root, &entry.old_path, &entry.new_path) {
        Ok(()) => {
            let _ = room.events.send(json!({
                "type":"file:renamed",
                "oldPath":entry.old_path,
                "newPath":entry.new_path
            }));
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            (StatusCode::CONFLICT, error.to_string()).into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

fn safe_path(root: &Path, relative: &str) -> Result<PathBuf, ()> {
    let path = Path::new(relative);
    if relative.trim().is_empty()
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(());
    }
    let mut current = root.to_path_buf();
    for part in path.components() {
        current.push(part.as_os_str());
        if let Ok(metadata) = std::fs::symlink_metadata(&current) {
            if metadata.file_type().is_symlink() {
                return Err(());
            }
        }
    }
    Ok(current)
}

async fn execute_project_file(root: &Path, path: &Path) -> Result<RunResult, String> {
    let file_name = path
        .file_name()
        .ok_or_else(|| "Имя решения некорректно".to_string())?;
    let run_root = root
        .join(".cwm-runs")
        .join(Uuid::new_v4().simple().to_string());
    tokio::fs::create_dir_all(&run_root)
        .await
        .map_err(|error| error.to_string())?;
    let isolated_path = run_root.join(file_name);
    if let Err(error) = tokio::fs::copy(path, &isolated_path).await {
        let _ = tokio::fs::remove_dir_all(&run_root).await;
        return Err(format!("Не удалось подготовить решение: {error}"));
    }
    let result = execute_isolated_file(&run_root, &isolated_path).await;
    let _ = tokio::fs::remove_dir_all(&run_root).await;
    result
}

async fn execute_isolated_file(root: &Path, path: &Path) -> Result<RunResult, String> {
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_lowercase();
    match ext.as_str() {
        "py" => {
            execute(
                Command::new(if cfg!(windows) { "python" } else { "python3" })
                    .arg(path)
                    .current_dir(root),
            )
            .await
        }
        "go" => execute(Command::new("go").arg("run").arg(path).current_dir(root)).await,
        "cpp" | "cc" | "cxx" => compile_and_run_cpp(root, path).await,
        "java" => compile_and_run_java(root, path).await,
        _ => Err("Поддерживаются Python, Go, C++ и Java".into()),
    }
}

async fn execute(command: &mut Command) -> Result<RunResult, String> {
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    match timeout(RUN_TIMEOUT, command.output()).await {
        Ok(Ok(output)) => Ok(RunResult {
            success: output.status.success(),
            exit_code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            timed_out: false,
        }),
        Ok(Err(error)) => Err(format!("Не удалось запустить программу: {error}")),
        Err(_) => Ok(RunResult {
            success: false,
            exit_code: None,
            stdout: String::new(),
            stderr: "Время выполнения превышено (15 секунд)".into(),
            timed_out: true,
        }),
    }
}

async fn compile_and_run_cpp(root: &Path, source: &Path) -> Result<RunResult, String> {
    let build = root.join(".cwm-build");
    tokio::fs::create_dir_all(&build)
        .await
        .map_err(|error| error.to_string())?;
    let binary = build.join(if cfg!(windows) {
        "solution.exe"
    } else {
        "solution"
    });
    let compiler = if cfg!(windows) { "g++" } else { "c++" };
    let compile = execute(
        Command::new(compiler)
            .arg("-std=c++20")
            .arg(source)
            .arg("-o")
            .arg(&binary)
            .current_dir(root),
    )
    .await?;
    if !compile.success {
        return Ok(compile);
    }
    execute(Command::new(binary).current_dir(root)).await
}

async fn compile_and_run_java(root: &Path, source: &Path) -> Result<RunResult, String> {
    let build = root.join(".cwm-build-java");
    tokio::fs::create_dir_all(&build)
        .await
        .map_err(|error| error.to_string())?;
    let source_text = tokio::fs::read_to_string(source)
        .await
        .map_err(|error| error.to_string())?;
    let normalized: String = source_text
        .chars()
        .map(|character| {
            if character.is_alphanumeric() || character == '_' {
                character
            } else {
                ' '
            }
        })
        .collect();
    let tokens: Vec<_> = normalized.split_whitespace().collect();
    let class_name = tokens
        .windows(3)
        .find(|parts| parts[0] == "public" && parts[1] == "class")
        .map(|parts| parts[2])
        .or_else(|| {
            tokens
                .windows(2)
                .find(|parts| parts[0] == "class")
                .map(|parts| parts[1])
        })
        .ok_or_else(|| "В решении Java не найден класс для запуска".to_string())?;
    let compile_source = root.join(format!("{class_name}.java"));
    if compile_source != source {
        tokio::fs::write(&compile_source, &source_text)
            .await
            .map_err(|error| error.to_string())?;
    }
    let compile = execute(
        Command::new("javac")
            .arg("-d")
            .arg(&build)
            .arg(&compile_source)
            .current_dir(root),
    )
    .await?;
    if !compile.success {
        return Ok(compile);
    }
    execute(
        Command::new("java")
            .arg("-cp")
            .arg(build)
            .arg(class_name)
            .current_dir(root),
    )
    .await
}

async fn websocket(
    State(room): State<Arc<RoomData>>,
    Query(query): Query<SocketQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    if query.code != room.info.invite_code {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let name = query
        .name
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "Гость".into());
    let is_host = query.host.unwrap_or(false);
    let id = if is_host {
        let participants = room.participants.read().await;
        match participants
            .iter()
            .find(|(_, participant)| participant.host)
            .map(|(id, _)| id.clone())
        {
            Some(id) => id,
            None => return StatusCode::FORBIDDEN.into_response(),
        }
    } else {
        let id = Uuid::new_v4().to_string();
        let mut participants = room.participants.write().await;
        if participants.len() >= MAX_PARTICIPANTS {
            return (StatusCode::FORBIDDEN, "В комнате уже 10 участников").into_response();
        }
        participants.insert(
            id.clone(),
            Participant {
                name: name.clone(),
                host: false,
            },
        );
        id
    };
    let _ = room
        .events
        .send(json!({"type":"participants", "participants":room_info(&room).await.participants}));
    ws.on_upgrade(move |socket| handle_socket(socket, room, id, name, is_host))
}

async fn handle_socket(
    socket: WebSocket,
    room: Arc<RoomData>,
    id: String,
    name: String,
    is_host: bool,
) {
    if is_host {
        if let Some(participant) = room.participants.write().await.get_mut(&id) {
            participant.name = name.clone();
        }
    }
    let (mut sender, mut receiver) = socket.split();
    let ready = json!({"type":"room:ready", "id":id, "name":name, "room":room_info(&room).await});
    if sender
        .send(Message::Text(ready.to_string().into()))
        .await
        .is_err()
    {
        return;
    }
    let mut events = room.events.subscribe();
    loop {
        tokio::select! {
            incoming = receiver.next() => {
                let Some(Ok(message)) = incoming else { break; };
                if let Message::Text(text) = message {
                    let Ok(mut event) = serde_json::from_str::<Value>(&text) else { continue; };
                    let kind = event.get("type").and_then(Value::as_str).unwrap_or("");
                    if kind == "presence" {
                        if let Some(object) = event.as_object_mut() { object.insert("participantId".into(), json!(id)); }
                        let _ = room.events.send(event);
                    } else if kind == "rename" {
                        let Some(new_name) = event.get("name").and_then(Value::as_str).map(str::trim).filter(|name| !name.is_empty()) else { continue; };
                        let new_name: String = new_name.chars().take(40).collect();
                        let renamed = {
                            let mut participants = room.participants.write().await;
                            if let Some(participant) = participants.get_mut(&id) {
                                participant.name = new_name;
                                true
                            } else { false }
                        };
                        if renamed {
                            let _ = room.events.send(json!({"type":"participants", "participants":room_info(&room).await.participants}));
                        }
                    }
                }
            }
            event = events.recv() => {
                let Ok(event) = event else { continue; };
                if event.get("participantId").and_then(Value::as_str) == Some(id.as_str()) { continue; }
                if sender.send(Message::Text(event.to_string().into())).await.is_err() { break; }
            }
        }
    }
    if !is_host {
        room.participants.write().await.remove(&id);
    }
    let _ = room
        .events
        .send(json!({"type":"participants", "participants":room_info(&room).await.participants}));
}
