// Prevents an extra console window from popping up on Windows in release builds.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::fs;
use std::path::PathBuf;

/// Locate (and ensure existence of) the app's data directory, and the
/// SQLCipher db path within it. Mirrors what MainActivity.kt does on
/// Android with `filesDir`.
fn app_data_dir() -> PathBuf {
    let mut dir = dirs::data_dir().expect("could not resolve OS data dir");
    dir.push("ZaoP2P");
    fs::create_dir_all(&dir).expect("failed to create app data dir");
    dir
}

fn db_path() -> String {
    app_data_dir().join("zao.db").to_string_lossy().to_string()
}

/// TEMPORARY key handling for Milestone 1, mirroring the Android shell's
/// current approach: a random key persisted in a plaintext file next to
/// the DB. This is NOT the final design. Before any real release, this
/// must be replaced with a key sealed via Windows DPAPI
/// (CryptProtectData), analogous to Android Keystore wrapping.
fn get_or_create_db_key() -> String {
    let key_path = app_data_dir().join("zao.key");
    if let Ok(existing) = fs::read_to_string(&key_path) {
        if !existing.trim().is_empty() {
            return existing.trim().to_string();
        }
    }
    let mut bytes = [0u8; 32];
    // Using the OS RNG the core crate already depends on (rand) rather
    // than adding a second RNG dependency here.
    use rand::RngCore;
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let key = hex::encode(bytes);
    fs::write(&key_path, &key).expect("failed to persist temp db key");
    key
}

#[tauri::command]
fn init_app_cmd() -> Result<String, String> {
    let path = db_path();
    let key = get_or_create_db_key();
    zao_transfer_core::ffi::init_app(&path, &key)
}

#[tauri::command]
fn get_identity_cmd() -> Result<String, String> {
    let path = db_path();
    let key = get_or_create_db_key();
    zao_transfer_core::ffi::get_identity(&path, &key)
}

fn downloads_dir() -> String {
    let dir = app_data_dir().join("downloads");
    fs::create_dir_all(&dir).expect("failed to create downloads dir");
    dir.to_string_lossy().to_string()
}

#[tauri::command]
fn start_networking_cmd() -> Result<String, String> {
    let path = db_path();
    let key = get_or_create_db_key();
    let display_name = format!("{} (Windows)", whoami::devicename());
    let downloads = downloads_dir();
    zao_transfer_core::ffi::start_networking(&path, &key, &display_name, &downloads)
}

#[tauri::command]
fn discover_peers_cmd() -> Result<String, String> {
    zao_transfer_core::ffi::discover_peers()
}

#[tauri::command]
fn networking_status_cmd() -> Result<String, String> {
    zao_transfer_core::ffi::networking_status()
}

#[tauri::command]
fn connect_to_peer_cmd(addr: String, expected_device_id: String) -> Result<String, String> {
    zao_transfer_core::ffi::connect_to_peer(&addr, &expected_device_id)
}

#[tauri::command]
fn connect_signaling_server_cmd(url: String) -> Result<String, String> {
    zao_transfer_core::ffi::connect_signaling_server(&url)
}

#[tauri::command]
fn connect_to_peer_via_internet_cmd(peer_device_id: String) -> Result<String, String> {
    zao_transfer_core::ffi::connect_to_peer_via_internet(&peer_device_id)
}

#[tauri::command]
fn send_text_message_cmd(peer_device_id: String, body: String) -> Result<String, String> {
    zao_transfer_core::ffi::send_text_message(&peer_device_id, &body)
}

#[tauri::command]
fn poll_events_cmd() -> Result<String, String> {
    zao_transfer_core::ffi::poll_events()
}

#[tauri::command]
fn get_conversation_history_cmd(peer_device_id: String, limit: u32) -> Result<String, String> {
    zao_transfer_core::ffi::get_conversation_history(&peer_device_id, limit)
}

#[tauri::command]
fn send_typing_indicator_cmd(
    peer_device_id: String,
    conversation_id: String,
    is_typing: bool,
) -> Result<String, String> {
    zao_transfer_core::ffi::send_typing_indicator(&peer_device_id, &conversation_id, is_typing)
}

#[tauri::command]
fn mark_message_read_cmd(peer_device_id: String, message_id: String) -> Result<String, String> {
    zao_transfer_core::ffi::mark_message_read(&peer_device_id, &message_id)
}

#[tauri::command]
fn connected_peers_cmd() -> Result<String, String> {
    zao_transfer_core::ffi::connected_peers()
}

#[tauri::command]
fn pause_transfer_cmd(transfer_id: String) -> Result<String, String> {
    zao_transfer_core::ffi::pause_transfer(&transfer_id)
}

#[tauri::command]
fn resume_transfer_cmd(transfer_id: String) -> Result<String, String> {
    zao_transfer_core::ffi::resume_transfer(&transfer_id)
}

#[tauri::command]
fn cancel_transfer_cmd(peer_device_id: String, transfer_id: String) -> Result<String, String> {
    zao_transfer_core::ffi::cancel_transfer(&peer_device_id, &transfer_id)
}

#[tauri::command]
fn get_transfer_progress_cmd(transfer_id: String) -> Result<String, String> {
    zao_transfer_core::ffi::get_transfer_progress(&transfer_id)
}

#[tauri::command]
fn send_file_cmd(peer_device_id: String, file_path: String, conversation_id: String) -> Result<String, String> {
    zao_transfer_core::ffi::send_file(&peer_device_id, &file_path, &conversation_id)
}

#[tauri::command]
fn send_folder_cmd(peer_device_id: String, folder_path: String, conversation_id: String) -> Result<String, String> {
    zao_transfer_core::ffi::send_folder(&peer_device_id, &folder_path, &conversation_id)
}

#[tauri::command]
#[allow(clippy::too_many_arguments)]
fn accept_file_cmd(
    from_device_id: String,
    transfer_id: String,
    message_id: String,
    file_name: String,
    file_size: u64,
    mime_type: String,
) -> Result<String, String> {
    zao_transfer_core::ffi::accept_file(
        &from_device_id,
        &transfer_id,
        &message_id,
        &file_name,
        file_size,
        &mime_type,
    )
}

#[tauri::command]
fn reject_file_cmd(from_device_id: String, transfer_id: String, reason: String) -> Result<String, String> {
    zao_transfer_core::ffi::reject_file(&from_device_id, &transfer_id, &reason)
}

fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            init_app_cmd,
            get_identity_cmd,
            start_networking_cmd,
            discover_peers_cmd,
            networking_status_cmd,
            connect_to_peer_cmd,
            connect_signaling_server_cmd,
            connect_to_peer_via_internet_cmd,
            send_text_message_cmd,
            poll_events_cmd,
            get_conversation_history_cmd,
            send_typing_indicator_cmd,
            mark_message_read_cmd,
            connected_peers_cmd,
            pause_transfer_cmd,
            resume_transfer_cmd,
            cancel_transfer_cmd,
            get_transfer_progress_cmd,
            send_file_cmd,
            send_folder_cmd,
            accept_file_cmd,
            reject_file_cmd
        ])
        .run(tauri::generate_context!())
        .expect("error while running Zao P2P Windows shell");
}
