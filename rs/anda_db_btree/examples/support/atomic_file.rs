//! Atomic file replacement for the example's single-writer store.
use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

pub fn write_atomically(path: &Path, data: &[u8]) -> io::Result<()> {
    replace(path, data, || Ok(()))
}

fn replace(
    path: &Path,
    data: &[u8],
    before_commit: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "file name required"))?;
    let (temporary, mut file) = loop {
        let mut temp_name = name.to_os_string();
        temp_name.push(format!(
            ".{}.{}.tmp",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let temporary = parent.join(temp_name);
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
        file.write_all(data)?;
        file.sync_all()?;
        drop(file);
        before_commit()?;
        fs::rename(&temporary, path)?;
        #[cfg(unix)]
        fs::File::open(parent)?.sync_all()?;
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
    use anda_db_btree::BTreeIndex;
    use futures::executor::block_on;
    use std::{
        collections::BTreeMap,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn no_op_and_failed_commits_preserve_the_old_file() {
        let dir = std::env::temp_dir().join(format!(
            "anda-btree-atomic-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("meta.cbor");
        let index = BTreeIndex::<u64, u64>::new("atomic".into(), None);
        index.insert(1, 10, 1).unwrap();
        let mut buckets = BTreeMap::new();
        block_on(index.flush_owned_with(
            1,
            |data| std::future::ready(write_atomically(&path, &data).map_err(Into::into)),
            |object, data| {
                buckets.insert(object, data);
                std::future::ready(Ok(()))
            },
        ))
        .unwrap();
        let committed = fs::read(&path).unwrap();
        let no_op = block_on(index.flush_owned_with(
            2,
            |_| -> std::future::Ready<Result<(), anda_db_btree::BoxError>> {
                panic!("clean flush must not touch metadata")
            },
            |_, _| std::future::ready(Ok(())),
        ))
        .unwrap();
        assert!(!no_op.saved);
        assert_eq!(fs::read(&path).unwrap(), committed);

        index.insert(2, 20, 3).unwrap();
        assert!(
            block_on(index.flush_owned_with(
                3,
                |_| -> std::future::Ready<Result<(), anda_db_btree::BoxError>> {
                    panic!("bucket failure must precede commit")
                },
                |_, _| std::future::ready(Err("bucket failed".into()))
            ))
            .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), committed);

        assert!(
            block_on(index.flush_owned_with(
                4,
                |data| {
                    std::future::ready(
                        replace(&path, &data, || Err(io::Error::other("before rename")))
                            .map_err(Into::into),
                    )
                },
                |object, data| {
                    buckets.insert(object, data);
                    std::future::ready(Ok(()))
                }
            ))
            .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), committed);
        assert_eq!(
            fs::read_dir(&dir).unwrap().count(),
            1,
            "failed temporary file must be cleaned"
        );
        let old = block_on(BTreeIndex::<u64, u64>::load_all(
            &committed[..],
            async |object| Ok(buckets.get(&object).cloned()),
        ))
        .unwrap();
        assert_eq!(old.keys(None, None), vec![10]);

        block_on(index.flush_owned_with(
            5,
            |data| std::future::ready(write_atomically(&path, &data).map_err(Into::into)),
            |object, data| {
                buckets.insert(object, data);
                std::future::ready(Ok(()))
            },
        ))
        .unwrap();
        let data = fs::read(&path).unwrap();
        let new = block_on(BTreeIndex::<u64, u64>::load_all(
            &data[..],
            async |object| Ok(buckets.get(&object).cloned()),
        ))
        .unwrap();
        assert_eq!(new.keys(None, None), vec![10, 20]);
        fs::remove_dir_all(dir).unwrap();
    }
}
