use once_cell::sync::Lazy;
use std::path::PathBuf;

static WORKING_DIR: Lazy<PathBuf> =
    Lazy::new(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));

pub fn get_directory() -> String {
    WORKING_DIR.to_string_lossy().to_string()
}
