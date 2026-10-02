use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

/// Append-only durable records. Database acknowledgement is separate from local acceptance.
/// A single service owns this directory; incomplete temporary writes are never replayed.
pub struct Journal {
    directory: PathBuf,
    next: u64,
    saved: u64,
    writable: bool,
    _lock: File,
}

impl Journal {
    pub fn open(directory: &Path) -> io::Result<Self> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(directory.join("lock"))?;
        lock.try_lock()
            .map_err(|_| io::Error::other("state directory already in use"))?;
        let mut ids = Vec::new();
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "record") {
                let id = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(|| io::Error::other("invalid journal filename"))?;
                if path.file_name().and_then(|s| s.to_str()) != Some(&format!("{id:020}.record")) {
                    return Err(io::Error::other("noncanonical journal filename"));
                }
                ids.push(id);
            }
        }
        ids.sort_unstable();
        if ids.iter().copied().ne(1..=ids.len() as u64) {
            return Err(io::Error::other("journal has missing or duplicate records"));
        }
        let saved = match fs::read_to_string(directory.join("saved")) {
            Ok(value) => value
                .parse()
                .map_err(|_| io::Error::other("invalid saved cursor"))?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e),
        };
        if saved > ids.len() as u64 {
            return Err(io::Error::other("saved cursor exceeds journal"));
        }
        Ok(Self {
            directory: directory.into(),
            next: ids.len() as u64 + 1,
            saved,
            writable: true,
            _lock: lock,
        })
    }

    pub fn append(&mut self, record: &[u8]) -> io::Result<u64> {
        let id = self.next;
        self.write_atomic(&format!("{id:020}.record"), record)?;
        self.next += 1;
        Ok(id)
    }

    pub fn read(&self, id: u64) -> io::Result<Vec<u8>> {
        if id == 0 || id >= self.next {
            return Err(io::Error::other("record outside journal"));
        }
        fs::read(self.directory.join(format!("{id:020}.record")))
    }

    pub fn writable(&self) -> bool {
        self.writable
    }

    pub fn last(&self) -> u64 {
        self.next - 1
    }
    pub fn saved(&self) -> u64 {
        self.saved
    }

    /// Called only after PostgreSQL confirms the complete prefix was committed.
    pub fn acknowledge(&mut self, id: u64) -> io::Result<()> {
        if id < self.saved || id >= self.next {
            return Err(io::Error::other("invalid acknowledgement"));
        }
        self.write_atomic("saved", id.to_string().as_bytes())?;
        self.saved = id;
        Ok(())
    }

    fn write_atomic(&mut self, name: &str, bytes: &[u8]) -> io::Result<()> {
        if !self.writable {
            return Err(io::Error::other(
                "journal requires recovery after a write failure",
            ));
        }
        // A rename followed by a failed directory fsync has an uncertain outcome.
        // Refuse further writes until reopening, rather than overwrite that record.
        self.writable = false;
        let temporary = self.directory.join("pending.tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(temporary, self.directory.join(name))?;
        File::open(&self.directory)?.sync_all()?;
        self.writable = true;
        Ok(())
    }
}
