use anyhow::Result;
use parity_scale_codec::{Decode, Encode, IoReader};
use std::borrow::Cow;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("this directory doesn't contain a wal")]
pub struct InvalidWal;

/// A wal entry.
///
/// Entries are 128 bytes. They have the format
/// u8 -> payload -> zero padding
enum Entry<'a, T: Clone + 'static> {
    /// An operation of a transaction.
    Op(Cow<'a, T>),
    /// Begins executing the transaction.
    ///
    /// Contains the next id counter after successfully executing the
    /// transaction.
    Begin(u64),
    /// Marks a transaction as executed or aborted.
    ///
    /// Contains the last next id counter if the transaction was aborted
    /// or the new next id counter if it was executed.
    End(u64),
}

impl<'a, T: Clone + Decode + Encode + 'static> Entry<'a, T> {
    fn write<W: Write>(&self, w: &mut W) -> Result<()> {
        let mut buf = [0u8; 128];
        match self {
            Self::Op(op) => {
                buf[0] = 0;
                op.encode_to(&mut &mut buf[1..]);
            }
            Self::Begin(i) => {
                buf[0] = 1;
                buf[1..9].copy_from_slice(&i.to_be_bytes()[..]);
            }
            Self::End(i) => {
                buf[0] = 2;
                buf[1..9].copy_from_slice(&i.to_be_bytes()[..]);
            }
        };
        w.write_all(&buf)?;
        Ok(())
    }

    fn read<R: Read + Seek>(mut r: &mut R) -> Result<Self> {
        let pos = r.seek(SeekFrom::Current(0))?;
        let mut code = [0];
        r.read_exact(&mut code)?;
        let entry = match code[0] {
            0 => Self::Op(Cow::Owned(T::decode(&mut IoReader(&mut r))?)),
            1 => {
                let mut buf = [0; 8];
                r.read_exact(&mut buf)?;
                Self::Begin(u64::from_be_bytes(buf))
            }
            2 => {
                let mut buf = [0; 8];
                r.read_exact(&mut buf)?;
                Self::End(u64::from_be_bytes(buf))
            }
            _ => return Err(InvalidWal.into()),
        };
        r.seek(SeekFrom::Start(pos + 128))?;
        Ok(entry)
    }
}

struct Log<T> {
    timestamp: u128,
    file: File,
    id: u64,
    entries: u64,
    replay: Option<(u64, Vec<T>)>,
}

impl<T: Clone + Decode + Encode + 'static> Log<T> {
    fn create(dir: &Path, id: u64) -> Result<Self> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = dir.join(timestamp.to_string());
        let mut file = OpenOptions::new().create(true).write(true).open(path)?;
        Entry::<()>::End(id).write(&mut file)?;
        file.sync_data()?;
        Ok(Self {
            timestamp,
            file,
            id,
            entries: 1,
            replay: None,
        })
    }

    fn open(dir: &Path, timestamp: u128) -> Result<Option<Self>> {
        let path = dir.join(timestamp.to_string());
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let len = file.seek(SeekFrom::End(0))?;
        let mut pos = file.seek(SeekFrom::Current(-((len % 128) as i64)))?;
        let mut replay = None;
        let id = loop {
            if pos < 128 {
                return Ok(None);
            }
            file.seek(SeekFrom::Current(-128))?;
            match Entry::<'static, T>::read(&mut file)? {
                Entry::End(id) => break id,
                Entry::Begin(id) => replay = Some((id, vec![])),
                Entry::Op(op) => {
                    if let Some(replay) = replay.as_mut() {
                        replay.1.push(op.into_owned());
                    }
                }
            }
            pos = file.seek(SeekFrom::Current(-128))?;
        };
        let pos = if let Some(replay) = replay.as_ref() {
            pos + (replay.1.len() as u64 + 1) * 128
        } else {
            pos
        };
        file.set_len(pos)?;

        Ok(Some(Self {
            timestamp,
            file,
            id,
            entries: pos / 128,
            replay,
        }))
    }

    fn remove(self, dir: &Path) -> Result<()> {
        Ok(fs::remove_file(dir.join(self.timestamp.to_string()))?)
    }

    fn replay(&mut self) -> Option<(u64, Vec<T>)> {
        self.replay.take()
    }

    fn write(&mut self, entry: &Entry<T>) -> Result<()> {
        if self.replay.is_some() {
            panic!();
        }
        entry.write(&mut self.file)?;
        if let Entry::End(id) = entry {
            self.id = *id;
        }
        self.entries += 1;
        Ok(())
    }

    fn sync_data(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    fn id(&self) -> u64 {
        self.id
    }

    fn entries(&self) -> u64 {
        self.entries
    }
}

/// Write-ahead log.
pub struct Wal<T> {
    path: PathBuf,
    log: Log<T>,
}

impl<T: Clone + Debug + Decode + Encode + 'static> Wal<T> {
    /// Opens or creates the wal and seeks to the last
    /// begin or end entry.
    pub fn open(path: PathBuf) -> Result<Self> {
        fs::create_dir_all(&path)?;
        let mut timestamps = vec![];
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                return Err(InvalidWal.into());
            }
            let file_name = entry.file_name();
            let file_name_str = file_name.to_str().ok_or(InvalidWal)?;
            let timestamp = u128::from_str(file_name_str).map_err(|_| InvalidWal)?;
            timestamps.push(timestamp);
        }
        let mut good_log = None;
        timestamps.sort_by(|a, b| a.cmp(b).reverse());
        for timestamp in timestamps {
            let log = Log::open(&path, timestamp)?;
            if good_log.is_none() && log.is_some() {
                good_log = log;
            } else {
                fs::remove_file(&path)?;
            }
        }
        let log = if let Some(log) = good_log {
            log
        } else {
            Log::create(&path, 0)?
        };
        Ok(Self { path, log })
    }

    /// The last committed next_id.
    pub fn id(&self) -> u64 {
        self.log.id()
    }

    /// Returns the begin id and all the operations before the last begin entry.
    pub fn replay(&mut self) -> Option<(u64, Vec<T>)> {
        self.log.replay()
    }

    /// Writes an operation to the wal.
    pub fn op(&mut self, op: &T) -> Result<()> {
        log::trace!("op {:?}", op);
        self.log.write(&Entry::Op(Cow::Borrowed(op)))
    }

    /// Writes a begin entry to the wal.
    ///
    /// The next_id that should be committed if the transaction succeeds.
    pub fn begin(&mut self, next_id: u64) -> Result<()> {
        log::trace!("begin {}", next_id);
        self.log.write(&Entry::Begin(next_id))?;
        self.log.sync_data()?;
        Ok(())
    }

    /// Writes an end entry to the wal.
    ///
    /// The next_id that is committed to the wal. If the transaction is
    /// executed successfully it will be the same as the begin next_id,
    /// otherwise it will be the next_id of the last end statement.
    pub fn end(&mut self, next_id: u64) -> Result<()> {
        log::trace!("end {}", next_id);
        self.log.write(&Entry::End(next_id))?;
        self.log.sync_data()?;
        if self.log.entries() > 10000 {
            let log = Log::create(&self.path, self.id())?;
            let old_log = std::mem::replace(&mut self.log, log);
            old_log.remove(&self.path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn test_log_no_begin() {
        let tmp = TempDir::new("wal").unwrap();
        let mut wal = Wal::<u64>::open(tmp.path().into()).unwrap();
        wal.op(&0).unwrap();
        wal.op(&1).unwrap();
        let mut wal = Wal::<u64>::open(tmp.path().into()).unwrap();
        assert_eq!(wal.replay(), None);
        assert_eq!(wal.id(), 0);
        assert_eq!(wal.log.entries(), 1);
    }

    #[test]
    fn test_log_replay() {
        let tmp = TempDir::new("wal").unwrap();
        let mut wal = Wal::<u64>::open(tmp.path().into()).unwrap();
        wal.op(&0).unwrap();
        wal.op(&1).unwrap();
        wal.begin(2).unwrap();
        let mut wal = Wal::<u64>::open(tmp.path().into()).unwrap();
        assert_eq!(wal.replay(), Some((2, vec![1, 0])));
        assert_eq!(wal.id(), 0);
        assert_eq!(wal.log.entries(), 4);
    }

    #[test]
    fn test_log_applied() {
        let tmp = TempDir::new("wal").unwrap();
        let mut wal = Wal::<u64>::open(tmp.path().into()).unwrap();
        wal.op(&0).unwrap();
        wal.op(&1).unwrap();
        wal.begin(2).unwrap();
        wal.end(2).unwrap();
        let mut wal = Wal::<u64>::open(tmp.path().into()).unwrap();
        assert_eq!(wal.replay(), None);
        assert_eq!(wal.id(), 2);
        assert_eq!(wal.log.entries(), 5);
    }
}
