use once_cell::sync::Lazy;
use std::path::PathBuf;

static WORKING_DIR: Lazy<PathBuf> =
    Lazy::new(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));

pub fn load_file_vec(path: &str, name: &str) -> Result<Vec<u8>, std::io::Error> {
    let dir = PathBuf::from(get_directory()).join(path);
    let file_path = dir.join(name);
    std::fs::read(file_path)
}

pub fn get_directory() -> String {
    WORKING_DIR.to_string_lossy().to_string()
}
