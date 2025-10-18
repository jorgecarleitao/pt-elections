use std::path::{Path, PathBuf};

use crate::BlobStorageProvider;

static ROOT: &'static str = "database/";

/// A [`BlobStorageProvider`] for local disk
pub struct LocalDisk;

#[async_trait::async_trait]
impl BlobStorageProvider for LocalDisk {
    async fn maybe_get(&self, blob_name: &str) -> Result<Option<Vec<u8>>, std::io::Error> {
        let path = PathBuf::from(ROOT).join(Path::new(blob_name));
        if path.try_exists()? {
            Ok(Some(std::fs::read(path)?))
        } else {
            Ok(None)
        }
    }

    async fn put(&self, blob_name: &str, contents: Vec<u8>) -> Result<(), std::io::Error> {
        let path = PathBuf::from(ROOT).join(Path::new(blob_name));
        let mut dir = path.clone();
        dir.pop();
        std::fs::create_dir_all(dir)?;
        std::fs::write(path, &contents)?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, std::io::Error> {
        let path = PathBuf::from(ROOT).join(Path::new(prefix));
        let mut paths = vec![];
        visit_dirs(path, &mut |dir| {
            let path = dir.path().to_str().unwrap().to_string();
            paths.push(path[ROOT.len()..].to_string())
        })?;
        Ok(paths)
    }

    async fn delete(&self, _prefix: &str) -> Result<(), std::io::Error> {
        todo!()
    }

    fn can_put(&self) -> bool {
        true
    }
}

fn visit_dirs<P: AsRef<Path>>(
    dir: P,
    cb: &mut dyn FnMut(&std::fs::DirEntry),
) -> std::io::Result<()> {
    if dir.as_ref().is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, cb)?;
            } else {
                cb(&entry);
            }
        }
    }
    Ok(())
}
