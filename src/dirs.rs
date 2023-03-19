use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Dirs {
    pub root_dir: PathBuf,
    pub log_root_dir: PathBuf,
    pub metadata_root_dir: PathBuf,
    pub data_root_dir: PathBuf,
    pub runtime_root_dir: PathBuf,
    pub cache_root_dir: PathBuf,
    pub secrets_root_dir: PathBuf,
}
