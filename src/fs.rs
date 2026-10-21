use std::io;
use std::path::{Path, PathBuf};

use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

/// Process-local scratch under durable `root`: contents rebuild from WAL at
/// restart, so boot wipes it wholesale and nothing fsyncs into it
pub fn scratch_dir(root: &Path) -> PathBuf {
    root.join("scratch")
}

/// Wipe and recreate `dir` wholesale
pub fn reset_dir(dir: &Path) -> io::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    std::fs::create_dir_all(dir)
}

/// Persist directory entry updates
pub async fn fsync_dir(dir: &Path) -> io::Result<()> {
    OpenOptions::new()
        .read(true)
        .open(dir)
        .await?
        .sync_all()
        .await
}

/// Check source system identifier a state file recorded against live one.
/// `Ok(true)` asks caller to rewrite a file predating the stamp
pub fn check_source(path: &Path, stored: Option<u64>, live: u64) -> io::Result<bool> {
    let Some(stored) = stored else {
        return Ok(true);
    };
    if stored != live {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} belongs to source system {stored}, live source is {live}; \
                 wipe the spill dir for a new source",
                path.display()
            ),
        ));
    }
    Ok(false)
}

/// Flush every dirty file and directory entry on `dir`'s filesystem, for a
/// tree too wide to fsync entry by entry
pub async fn sync_filesystem(dir: &Path) -> io::Result<()> {
    let f = tokio::fs::File::open(dir).await?.into_std().await;
    tokio::task::spawn_blocking(move || syncfs(&f))
        .await
        .map_err(io::Error::other)?
}

#[cfg(target_os = "linux")]
pub fn syncfs(f: &std::fs::File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: `f` owns the fd for the whole call
    if unsafe { libc::syncfs(f.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
pub fn syncfs(_f: &std::fs::File) -> io::Result<()> {
    unreachable!("walshadow is Linux-only")
}

/// Crash-safe replace: write+fsync `{name}.tmp`, rename over `{name}`,
/// fsync dir so rename survives power loss. Reader sees old-complete or
/// new-complete file, never a torn write; a crash between write and
/// rename leaves a stale `.tmp` no boot path reads.
pub async fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp_path = dir.join(format!("{name}.tmp"));
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_path)
        .await?;
    f.write_all(bytes).await?;
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(&tmp_path, dir.join(name)).await?;
    fsync_dir(dir).await
}

/// Blocking [`fsync_dir`], for code already off the runtime
pub fn sync_dir_blocking(dir: &Path) -> io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// Blocking [`write_private`], for code already off the runtime (or with
/// none, like a panic hook)
pub fn write_private_blocking(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let tmp_path = dir.join(format!("{name}.tmp"));
    match std::fs::remove_file(&tmp_path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp_path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp_path, dir.join(name))?;
    sync_dir_blocking(dir)
}

/// [`write_atomic`] for files only the daemon may read: the temp file is
/// created 0600 before any byte lands in it, and an existing temp is
/// replaced rather than reused with its old mode
pub async fn write_private(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp_path = dir.join(format!("{name}.tmp"));
    match tokio::fs::remove_file(&tmp_path).await {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp_path)
        .await?;
    f.write_all(bytes).await?;
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(&tmp_path, dir.join(name)).await?;
    fsync_dir(dir).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_private_is_owner_only_and_replaces() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        // A stale temp with wide permissions is replaced, not reused
        std::fs::write(dir.path().join("k.tmp"), b"stale").unwrap();
        std::fs::set_permissions(
            dir.path().join("k.tmp"),
            std::fs::Permissions::from_mode(0o666),
        )
        .unwrap();
        write_private(dir.path(), "k", b"one").await.unwrap();
        write_private(dir.path(), "k", b"two").await.unwrap();
        let path = dir.path().join("k");
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert!(!dir.path().join("k.tmp").exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_atomic_replaces_and_cleans_tmp() {
        let tmp = tempfile::tempdir().unwrap();
        write_atomic(tmp.path(), "f.toml", b"a = 1\n")
            .await
            .unwrap();
        write_atomic(tmp.path(), "f.toml", b"a = 2\n")
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(tmp.path().join("f.toml")).unwrap(),
            b"a = 2\n"
        );
        assert!(!tmp.path().join("f.toml.tmp").exists());
    }

    #[test]
    fn reset_dir_wipes_nested_contents() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = scratch_dir(tmp.path());
        std::fs::create_dir_all(dir.join("nested")).unwrap();
        std::fs::write(dir.join("nested/a.bin"), b"x").unwrap();
        std::fs::write(tmp.path().join("durable.toml"), b"x").unwrap();
        reset_dir(&dir).unwrap();
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        assert!(tmp.path().join("durable.toml").exists());
        reset_dir(&tmp.path().join("absent")).unwrap();

        // Refuse to replace a non-directory
        let file = tmp.path().join("durable.toml");
        assert!(reset_dir(&file).is_err());
        assert!(file.is_file());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_filesystem_flushes_existing_dir_only() {
        let tmp = tempfile::tempdir().unwrap();
        sync_filesystem(tmp.path()).await.unwrap();
        let err = sync_filesystem(&tmp.path().join("absent"))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
    #[tokio::test]
    async fn private_writes_replace_stale_temps_and_refuse_directory_temps() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("key.tmp"), b"stale").unwrap();
        write_private_blocking(dir.path(), "key", b"fresh").unwrap();
        assert_eq!(std::fs::read(dir.path().join("key")).unwrap(), b"fresh");
        std::fs::create_dir(dir.path().join("blocked.tmp")).unwrap();
        assert!(write_private_blocking(dir.path(), "blocked", b"no").is_err());
        assert!(write_private(dir.path(), "blocked", b"no").await.is_err());
        assert!(!dir.path().join("blocked").exists());
        assert_eq!(std::fs::read(dir.path().join("key")).unwrap(), b"fresh");
    }
}
