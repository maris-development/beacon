//! Builds plugin archives for the plugin tests.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};

use beacon_core::plugins::archive::{pack_dir, sha256_hex, unpack};
use beacon_core::plugins::InstallRecord;

/// Unpacks `archive` into `<plugins_dir>/<name>/` and writes its `install.json`, without checks.
pub fn put_plugin_folder(plugins_dir: &Path, name: &str, archive: &[u8]) {
    let dir = plugins_dir.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    unpack(archive, &dir).unwrap();
    write_record(&dir, name, &sha256_hex(archive));
}

/// Writes an `install.json` for plugin `name` into `dir`.
pub fn write_record(dir: &Path, name: &str, sha256: &str) {
    let record = InstallRecord {
        name: name.to_string(),
        version: "0.1.0".to_string(),
        source: "test".to_string(),
        sha256: sha256.to_string(),
        installed_at: "2026-10-24T00:00:00Z".to_string(),
    };
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(
        dir.join("install.json"),
        serde_json::to_vec(&record).unwrap(),
    )
    .unwrap();
}

/// The `install.json` of plugin `name` in `plugins_dir`.
pub fn read_record(plugins_dir: &Path, name: &str) -> InstallRecord {
    let bytes = std::fs::read(plugins_dir.join(name).join("install.json")).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// The repository root, two levels above `beacon-db/beacon-core`.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The example plugin fixture directory.
fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/example-plugin")
}

/// Builds the example native library once per process and returns its path.
fn example_library(wrong_abi: bool) -> PathBuf {
    static RIGHT: OnceLock<PathBuf> = OnceLock::new();
    static WRONG: OnceLock<PathBuf> = OnceLock::new();
    static BUILD: Mutex<()> = Mutex::new(());
    let cell = if wrong_abi { &WRONG } else { &RIGHT };
    cell.get_or_init(|| {
        // Both variants share one target folder, so the second build reuses the dependencies.
        let _build = BUILD.lock().unwrap_or_else(|e| e.into_inner());
        let target_dir = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| repo_root().join("target"))
            .join("plugin-example");
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let mut command = Command::new(cargo);
        command
            .args(["build", "--release", "--manifest-path"])
            .arg(fixture_dir().join("native/Cargo.toml"))
            .arg("--target-dir")
            .arg(&target_dir);
        if wrong_abi {
            command.args(["--features", "wrong-abi"]);
        }
        let status = command
            .status()
            .expect("run cargo build for the example plugin");
        assert!(status.success(), "cargo build of the example plugin failed");
        let file = format!(
            "{}example_native{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        );
        // Each variant overwrites the build output, so keep a copy per variant.
        let variant = if wrong_abi { "wrong-abi" } else { "right-abi" };
        let copy_dir = target_dir.join(variant);
        std::fs::create_dir_all(&copy_dir).unwrap();
        let copy = copy_dir.join(&file);
        std::fs::copy(target_dir.join("release").join(&file), &copy)
            .expect("copy the example plugin library");
        copy
    })
    .clone()
}

/// The example plugin as an archive, with its native library in `lib/`.
pub fn example_plugin(wrong_abi: bool) -> Vec<u8> {
    let dir = example_plugin_dir(wrong_abi);
    pack_dir(dir.path()).expect("pack plugin dir")
}

/// The example plugin as an unpacked directory, with its native library in `lib/`.
pub fn example_plugin_dir(wrong_abi: bool) -> tempfile::TempDir {
    let library = example_library(wrong_abi);
    let source = fixture_dir();
    let dir = tempfile::tempdir().expect("create plugin source dir");
    std::fs::create_dir_all(dir.path().join("sql")).unwrap();
    std::fs::create_dir_all(dir.path().join("lib")).unwrap();
    for file in ["beacon-plugin.toml", "sql/functions.sql"] {
        std::fs::copy(source.join(file), dir.path().join(file)).unwrap();
    }
    std::fs::copy(
        &library,
        dir.path().join("lib").join(library.file_name().unwrap()),
    )
    .unwrap();
    dir
}

/// Packs a plugin directory with `files` as `(relative path, contents)` pairs.
pub fn plugin_archive(files: &[(&str, &str)]) -> Vec<u8> {
    let dir = tempfile::tempdir().expect("create plugin source dir");
    for (path, contents) in files {
        super::write_file(&dir.path().join(path), contents);
    }
    pack_dir(dir.path()).expect("pack plugin dir")
}

/// The manifest text of a SQL-only plugin.
pub fn sql_manifest(name: &str, version: &str, beacon: &str, files: &[&str]) -> String {
    let files = files
        .iter()
        .map(|file| format!("\"{file}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[plugin]\nname = \"{name}\"\nversion = \"{version}\"\nbeacon = \"{beacon}\"\n\n[sql]\nfiles = [{files}]\n")
}

/// A SQL-only plugin with `sql` as `sql/functions.sql`.
pub fn sql_plugin(name: &str, version: &str, beacon: &str, sql: &str) -> Vec<u8> {
    let manifest = sql_manifest(name, version, beacon, &["sql/functions.sql"]);
    plugin_archive(&[
        ("beacon-plugin.toml", &manifest),
        ("sql/functions.sql", sql),
    ])
}

/// Serves the files of a directory over HTTP/1.1 on 127.0.0.1 until it drops.
pub struct FileServer {
    pub base_url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FileServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Starts a `FileServer` for `dir`. A missing file gets 404.
pub async fn serve_dir(dir: PathBuf) -> FileServer {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the file server");
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let dir = dir.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let text = String::from_utf8_lossy(&request);
                let path = text.split_whitespace().nth(1).unwrap_or("/").to_string();
                let file = path.trim_start_matches('/');
                let body = if file.is_empty() || file.contains("..") {
                    None
                } else {
                    std::fs::read(dir.join(file)).ok()
                };
                let (status, body) = match body {
                    Some(body) => ("200 OK", body),
                    None => ("404 Not Found", Vec::new()),
                };
                let head = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(&body).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    FileServer {
        base_url: format!("http://127.0.0.1:{port}"),
        task,
    }
}
