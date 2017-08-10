
pub use self::transaction::{Transaction, TransactionOptions};
use db::{Inner, DBIterator, DBRawIterator, IteratorMode};
use super::{Options, Error, ReadOptions, WriteOptions, DBVector};
use ffi;

use libc::{c_char, size_t};
use std::ffi::CString;
use std::fs;
use std::path::Path;

unsafe impl Send for TransactionDB {}
unsafe impl Sync for TransactionDB {}

pub mod transaction;

pub struct TransactionDB {
    pub inner: *mut ffi::rocksdb_transactiondb_t,
    // path: PathBuf,
}

pub struct TransactionDBOptions {
    inner: *mut ffi::rocksdb_transactiondb_options_t,
}

pub struct Snapshot<'a> {
    db: &'a TransactionDB,
    inner: *const ffi::rocksdb_snapshot_t,
}

impl TransactionDB {
    /// Open a transactional database with default options.
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let mut opts = Options::default();
        let transaction_db_opts = TransactionDBOptions::default();
        opts.create_if_missing(true);
        Self::open(&opts, &transaction_db_opts, path)
    }

    /// Open the transactional database with the specified options.
    pub fn open<P: AsRef<Path>>(
        opts: &Options,
        txn_db_opts: &TransactionDBOptions,
        path: P,
    ) -> Result<TransactionDB, Error> {
        let path = path.as_ref();
        let cpath = match CString::new(path.to_string_lossy().as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err(Error::new(
                    "Failed to convert path to CString \
                                       when opening DB."
                        .to_owned(),
                ))
            }
        };

        if let Err(e) = fs::create_dir_all(&path) {
            return Err(Error::new(format!(
                "Failed to create RocksDB \
                                           directory: `{:?}`.",
                e
            )));
        }

        let db: *mut ffi::rocksdb_transactiondb_t = unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_open(
                opts.inner,
                txn_db_opts.inner,
                cpath.as_ptr() as *const _
            ))
        };

        if db.is_null() {
            return Err(Error::new("Could not initialize database.".to_owned()));
        }

        Ok(TransactionDB {
            inner: db,
            // path: path.to_path_buf(),
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let opts = ReadOptions::default();
        self.get_opt(key, &opts)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let w_opts = WriteOptions::default();
        self.put_opt(key, value, &w_opts)
    }

    pub fn put_opt(&self, key: &[u8], value: &[u8], w_opts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_put(
                self.inner,
                w_opts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t
            ));
            Ok(())
        }
    }

    pub fn get_opt(&self, key: &[u8], read_opts: &ReadOptions) -> Result<Option<DBVector>, Error> {
        if read_opts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. \
                                   This is a fairly trivial call, and its \
                                   failure may be indicative of a \
                                   mis-compiled or mis-loaded RocksDB \
                                   library."
                    .to_owned(),
            ));
        }

        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transactiondb_get(
                self.inner,
                read_opts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut val_len
            )) as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }

    

    pub fn transaction_begin(
        &self,
        w_opts: &WriteOptions,
        txn_opts: &TransactionOptions,
    ) -> Transaction {
        Transaction::new(self, w_opts, txn_opts)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new(self)
    }

    pub fn destroy<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        let cpath = CString::new(path.as_ref().to_string_lossy().as_bytes()).unwrap();
        unsafe {
            ffi_try!(ffi::rocksdb_destroy_db(opts.inner, cpath.as_ptr()));
        }
        Ok(())
    }

    pub fn repair<P: AsRef<Path>>(opts: Options, path: P) -> Result<(), Error> {
        let cpath = CString::new(path.as_ref().to_string_lossy().as_bytes()).unwrap();
        unsafe {
            ffi_try!(ffi::rocksdb_repair_db(opts.inner, cpath.as_ptr()));
        }
        Ok(())
    }
}

impl Drop for TransactionDB {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transactiondb_close(self.inner);
        }
    }
}

impl<'a> Snapshot<'a> {
    pub fn new(db: &TransactionDB) -> Snapshot {
        let snapshot = unsafe { ffi::rocksdb_transactiondb_create_snapshot(db.inner) };
        Snapshot {
            db: db,
            inner: snapshot,
        }
    }

    pub fn iterator(&self, mode: IteratorMode) -> DBIterator {
        let mut readopts = ReadOptions::default();
        let writeopts = WriteOptions::default();
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_begin(&writeopts, &txn_opts);
        readopts.set_snapshot(self);
        DBIterator::new_txn(&txn, &readopts, mode)
    }

    pub fn raw_iterator(&self) -> DBRawIterator {
        let mut readopts = ReadOptions::default();
        let writeopts = WriteOptions::default();
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_begin(&writeopts, &txn_opts);
        readopts.set_snapshot(self);
        DBRawIterator::new_txn(&txn, &readopts)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        self.db.get_opt(key, &readopts)
    }
}

impl<'a> Drop for Snapshot<'a> {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transactiondb_release_snapshot(self.db.inner, self.inner);
        }
    }
}

impl<'a> Inner for Snapshot<'a> {
    fn get_inner(&self) -> *const ffi::rocksdb_snapshot_t {
        self.inner
    }
}

impl TransactionDBOptions {

    pub fn set_max_num_locks(&mut self, max_num_locks: i64) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_max_num_locks(self.inner, max_num_locks);
        }
    }

    pub fn set_num_stripes(&mut self, num_stripes: usize) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_num_stripes(self.inner, num_stripes);
        }
    }

    pub fn set_transaction_lock_timeout(&mut self, txn_lock_timeout: i64) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_transaction_lock_timeout(self.inner,
                                                                            txn_lock_timeout);
        }
    }

    pub fn set_default_lock_timeout(&mut self, default_lock_timeout: i64) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_default_lock_timeout(self.inner,
                                                                        default_lock_timeout);
        }
    }

}

impl Default for TransactionDBOptions {
    fn default() -> TransactionDBOptions {
        unsafe {
            let transaction_db_options = ffi::rocksdb_transactiondb_options_create();
            if transaction_db_options.is_null() {
                panic!("couldn't create TransactionDB options");
            }
            Self { inner: transaction_db_options }
        }
    }
}

impl Drop for TransactionDBOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transactiondb_options_destroy(self.inner);
        }
    }
}
