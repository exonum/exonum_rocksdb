use ColumnFamily;
use DBIterator;
use DBRawIterator;
use DBVector;
use Error;
use IteratorMode;
use Options;
use ReadOptions;
use WriteOptions;
use db::Inner;
use transaction::Transaction;
use utils;

use std::collections::BTreeMap;
use std::ffi::CString;
use std::path::Path;
use std::ptr;

use ffi;
use libc::{c_uchar, c_int};

pub struct OptimisticTransactionDB {
    pub inner: *mut ffi::rocksdb_optimistictransactiondb_t,
    base_db: *mut ffi::rocksdb_t,
    cfs: BTreeMap<String, ColumnFamily>,
}

impl OptimisticTransactionDB {
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_if_missing(true);
        Self::open(&options, path)
    }

    pub fn open<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Self, Error> {
        let path = path.as_ref();
        let cpath = utils::to_cpath(path)?;
        let db: *mut ffi::rocksdb_optimistictransactiondb_t = unsafe {
            ffi_try!(ffi::rocksdb_optimistictransactiondb_open(
                opts.inner,
                cpath.as_ptr() as *const _
            ))
        };

        if db.is_null() {
            return Err(Error::new("Could not initialize database.".to_owned()));
        }

        let base_db = unsafe { ffi::rocksdb_optimistictransactiondb_get_base_db(db) };

        Ok(OptimisticTransactionDB {
            inner: db,
            base_db: base_db,
            cfs: BTreeMap::new(),
        })
    }

    pub fn open_cf<P: AsRef<Path>>(opts: &Options, path: P, cfs: &[&str]) -> Result<Self, Error> {
        let path = path.as_ref();
        let cpath = utils::to_cpath(path)?;
        let db: *mut ffi::rocksdb_optimistictransactiondb_t;
        let mut cf_map = BTreeMap::new();

        if cfs.is_empty() {
            unsafe {
                db = ffi_try!(ffi::rocksdb_optimistictransactiondb_open(
                    opts.inner,
                    cpath.as_ptr() as *const _
                ));
            }
        } else {
            let mut cfs_v = cfs.to_vec();
            // Always open the default column family.
            if !cfs_v.contains(&"default") {
                cfs_v.push("default");
            }

            // We need to store our CStrings in an intermediate vector
            // so that their pointers remain valid.
            let c_cfs: Vec<CString> = cfs_v
                .iter()
                .map(|cf| CString::new(cf.as_bytes()).unwrap())
                .collect();

            let cfnames: Vec<_> = c_cfs.iter().map(|cf| cf.as_ptr()).collect();

            // These handles will be populated by DB.
            let mut cfhandles: Vec<_> = cfs_v.iter().map(|_| ptr::null_mut()).collect();

            // TODO(tyler) allow options to be passed in.
            let cfopts: Vec<_> = cfs_v
                .iter()
                .map(|_| unsafe { ffi::rocksdb_options_create() as *const _ })
                .collect();

            unsafe {
                db = ffi_try!(ffi::rocksdb_optimistictransactiondb_open_column_families(
                    opts.inner,
                    cpath.as_ptr() as *const _,
                    cfs_v.len() as c_int,
                    cfnames.as_ptr() as *const _,
                    cfopts.as_ptr(),
                    cfhandles.as_mut_ptr()
                ));
            }

            for handle in &cfhandles {
                if handle.is_null() {
                    return Err(Error::new(
                        "Received null column family \
                                           handle from DB."
                            .to_owned(),
                    ));
                }
            }

            for (n, h) in cfs_v.iter().zip(cfhandles) {
                cf_map.insert(n.to_string(), ColumnFamily { inner: h });
            }
        }

        if db.is_null() {
            return Err(Error::new("Could not initialize database.".to_owned()));
        }

        let base_db = unsafe { ffi::rocksdb_optimistictransactiondb_get_base_db(db) };

        Ok(OptimisticTransactionDB {
            inner: db,
            base_db: base_db,
            cfs: cf_map,
        })
    }

    pub fn transaction_begin(
        &self,
        w_opts: &WriteOptions,
        txn_opts: &OptimisticTransactionOptions,
    ) -> Transaction {
        Transaction::new_optimistic(self, w_opts, txn_opts)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new(self)
    }

    pub fn create_cf(&mut self, name: &str, opts: &Options) -> Result<ColumnFamily, Error> {
        let cname = utils::to_cpath(Path::new(name))?;
        let cf = unsafe {
            let cf_handler = ffi_try!(ffi::rocksdb_create_column_family(
                self.base_db,
                opts.inner,
                cname.as_ptr()
            ));
            let cf = ColumnFamily { inner: cf_handler };
            self.cfs.insert(name.to_string(), cf);
            cf
        };
        Ok(cf)
    }

    pub fn cf_handle(&self, name: &str) -> Option<ColumnFamily> {
        self.cfs.get(name).cloned()
    }

    pub fn drop_cf(&mut self, name: &str) -> Result<(), Error> {
        if let Some(cf) = self.cfs.get(name) {
            unsafe {
                ffi_try!(ffi::rocksdb_drop_column_family(self.base_db, cf.inner));
            }
            Ok(())
        } else {
            Err(Error::new(
                format!("Invalid column family: {}", name).to_owned(),
            ))
        }
    }

    pub fn destroy<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        let cpath = utils::to_cpath(path.as_ref())?;
        unsafe {
            ffi_try!(ffi::rocksdb_destroy_db(opts.inner, cpath.as_ptr()));
        }
        Ok(())
    }
}

impl Drop for OptimisticTransactionDB {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_optimistictransactiondb_close_base_db(self.base_db);
            ffi::rocksdb_optimistictransactiondb_close(self.inner);
        }
    }
}

pub struct Snapshot<'a> {
    db: &'a OptimisticTransactionDB,
    inner: *const ffi::rocksdb_snapshot_t,
}

impl<'a> Snapshot<'a> {
    pub fn new(db: &OptimisticTransactionDB) -> Snapshot {
        let snapshot = unsafe { ffi::rocksdb_create_snapshot(db.base_db) };
        Snapshot {
            db: db,
            inner: snapshot,
        }
    }

    pub fn iterator(&self, mode: IteratorMode) -> DBIterator {
        let mut r_opts = ReadOptions::default();
        let w_opts = WriteOptions::default();
        let mut txn_opts = OptimisticTransactionOptions::default();
        txn_opts.set_snapshot(true);
        r_opts.set_snapshot(self);
        let txn = self.db.transaction_begin(&w_opts, &txn_opts);
        DBIterator::new_txn(&txn, &r_opts, mode)
    }

    pub fn iterator_cf(
        &self,
        cf_handle: ColumnFamily,
        mode: IteratorMode,
    ) -> Result<DBIterator, Error> {
        let mut r_opts = ReadOptions::default();
        let w_opts = WriteOptions::default();
        let mut txn_opts = OptimisticTransactionOptions::default();
        txn_opts.set_snapshot(true);
        r_opts.set_snapshot(self);
        let txn = self.db.transaction_begin(&w_opts, &txn_opts);
        DBIterator::new_txn_cf(&txn, cf_handle, &r_opts, mode)
    }

    pub fn raw_iterator(&self) -> DBRawIterator {
        let mut r_opts = ReadOptions::default();
        let w_opts = WriteOptions::default();
        let mut txn_opts = OptimisticTransactionOptions::default();
        txn_opts.set_snapshot(true);
        r_opts.set_snapshot(self);
        let txn = self.db.transaction_begin(&w_opts, &txn_opts);
        DBRawIterator::new_txn(&txn, &r_opts)
    }

    pub fn raw_iterator_cf(&self, cf_handle: ColumnFamily) -> Result<DBRawIterator, Error> {
        let mut r_opts = ReadOptions::default();
        let w_opts = WriteOptions::default();
        let mut txn_opts = OptimisticTransactionOptions::default();
        txn_opts.set_snapshot(true);
        r_opts.set_snapshot(self);
        let txn = self.db.transaction_begin(&w_opts, &txn_opts);
        DBRawIterator::new_txn_cf(&txn, cf_handle, &r_opts)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let mut r_opts = ReadOptions::default();
        let w_opts = WriteOptions::default();
        let mut txn_opts = OptimisticTransactionOptions::default();
        txn_opts.set_snapshot(true);
        r_opts.set_snapshot(self);
        let txn = self.db.transaction_begin(&w_opts, &txn_opts);
        txn.get_opt(key, &r_opts)
    }

    pub fn get_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let mut r_opts = ReadOptions::default();
        let w_opts = WriteOptions::default();
        let mut txn_opts = OptimisticTransactionOptions::default();
        txn_opts.set_snapshot(true);
        r_opts.set_snapshot(self);
        let txn = self.db.transaction_begin(&w_opts, &txn_opts);
        txn.get_cf_opt(key, cf, &r_opts)
    }
}

impl<'a> Drop for Snapshot<'a> {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_release_snapshot(self.db.base_db, self.inner);
        }
    }
}

impl<'a> Inner for Snapshot<'a> {
    fn get_inner(&self) -> *const ffi::rocksdb_snapshot_t {
        self.inner
    }
}

pub struct OptimisticTransactionOptions {
    pub inner: *mut ffi::rocksdb_optimistictransaction_options_t,
}

impl OptimisticTransactionOptions {
    pub fn set_snapshot(&mut self, snapshot: bool) {
        unsafe {
            ffi::rocksdb_optimistictransaction_options_set_set_snapshot(
                self.inner,
                snapshot as c_uchar,
            );
        }
    }
}

impl Default for OptimisticTransactionOptions {
    fn default() -> Self {
        OptimisticTransactionOptions {
            inner: unsafe { ffi::rocksdb_optimistictransaction_options_create() },
        }
    }
}

impl Drop for OptimisticTransactionOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_optimistictransaction_options_destroy(self.inner);
        }
    }
}
