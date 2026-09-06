//! Single-writer local-file adapter for the manifest commit protocol.
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

pub fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    replace_file(path, |file| file.write_all(data))
}

fn replace_file(path: &Path, write: impl FnOnce(&mut File) -> io::Result<()>) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::other("missing file name"))?;
    replace_file_with_counter(path, parent, name, &NEXT_TEMP, write)
}

fn replace_file_with_counter(
    path: &Path,
    parent: &Path,
    name: &std::ffi::OsStr,
    next: &AtomicU64,
    write: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<()> {
    // A crash may leave the previous process's staging file behind. PIDs are
    // routinely reused (and are often always 1 in containers), so keep trying
    // fresh sequence values rather than turning one orphan into a restart loop.
    let (temporary, mut file) = loop {
        let mut temporary_name = name.to_os_string();
        temporary_name.push(format!(
            ".{}.{}.tmp",
            std::process::id(),
            next.fetch_add(1, Ordering::Relaxed)
        ));
        let temporary = parent.join(temporary_name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => break (temporary, file),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
    };
    let result = (|| {
        write(&mut file)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)?;
        // On Unix the directory entry must also reach durable storage.
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_staging_write_preserves_previous_file() {
        let dir = std::env::temp_dir().join(format!(
            "tfs-atomic-{}-{}",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("metadata.cbor");
        write_atomic(&path, b"old metadata").unwrap();
        let result = replace_file(&path, |file| {
            file.write_all(b"partial new metadata")?;
            Err(io::Error::other("injected write failure"))
        });
        assert!(result.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"old metadata");
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn skips_a_temporary_file_left_by_a_crashed_process() {
        let dir = std::env::temp_dir().join(format!(
            "tfs-atomic-stale-{}-{}",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("metadata.cbor");
        let name = path.file_name().unwrap();
        let counter = AtomicU64::new(0);
        let mut stale_name = name.to_os_string();
        stale_name.push(format!(".{}.0.tmp", std::process::id()));
        fs::write(dir.join(stale_name), b"orphaned staging data").unwrap();

        replace_file_with_counter(&path, &dir, name, &counter, |file| {
            file.write_all(b"committed metadata")
        })
        .unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"committed metadata");
        // The pre-existing orphan is deliberately not removed: it may belong
        // to another still-running writer. It must only be skipped.
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 2);
        fs::remove_dir_all(dir).unwrap();
    }
}
