use super::{WriteOptions, Error, DBVector, ReadOptions};
use transaction_db::TransactionDB;
use db::{DBIterator, IteratorMode};
use ffi;

use libc::{c_char, size_t, c_uchar};
use std::ptr::null_mut;

pub struct Transaction {
    pub inner: *mut ffi::rocksdb_transaction_t,
}

pub struct TransactionOptions {
    inner: *mut ffi::rocksdb_transaction_options_t,
}

impl Transaction {
    pub fn new(
        db: &TransactionDB,
        options: &WriteOptions,
        txn_options: &TransactionOptions,
    ) -> Self {
        unsafe {
            Transaction {
                inner: ffi::rocksdb_transaction_begin(
                    db.inner,
                    options.inner,
                    txn_options.inner,
                    null_mut(),
                ),
            }
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_put(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t
            ));
            Ok(())
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let opts = ReadOptions::default();
        self.get_opt(key, &opts)
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
            let val = ffi_try!(ffi::rocksdb_transaction_get(
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

    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_delete(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t
            ));
            Ok(())
        }
    }

    pub fn commit(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_commit(self.inner));
            Ok(())
        }
    }

    pub fn rollback(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_rollback(self.inner));
            Ok(())
        }
    }

    pub fn iterator(&self) -> DBIterator {
        let opts = ReadOptions::default();
        self.iterator_opt(&opts)
    }

    pub fn iterator_opt(&self, opts: &ReadOptions) -> DBIterator {
        DBIterator::new_txn(self, &opts, IteratorMode::Start)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transaction_destroy(self.inner);
        }
    }
}

impl TransactionOptions {
    /// If a transaction has a snapshot set, the transaction will ensure that
    /// any keys successfully written(or fetched have not been modified outside
    /// of this transaction since the time the snapshot was set.
    /// If a snapshot has not been set, the transaction guarantees that keys have
    /// not been modified since the time each key was first written.
    ///
    /// Using set_snapshot(true) will provide stricter isolation guarantees at the
    /// expense of potentially more transaction failures due to conflicts with
    /// other writes.
    ///
    /// Calling set_snapshot(true) has no effect on keys written before this function
    /// has been called.
    ///
    /// set_snapshot() may be called multiple times if you would like to change
    /// the snapshot used for different operations in this transaction.
    ///
    /// Default: `false`
    ///
    /// # Example
    ///
    /// ```
    /// use exonum_rocksdb::TransactionOptions;
    ///
    /// let mut opts = TransactionOptions::default();
    /// opts.set_snapshot(true);
    /// ```
    pub fn set_snapshot(&mut self, v: bool) {
        unsafe {
            ffi::rocksdb_transaction_options_set_set_snapshot(self.inner, v as c_uchar);
        }
    }

    pub fn set_deadlock_detect(&mut self, v: bool) {
        unsafe {
            ffi::rocksdb_transaction_options_set_deadlock_detect(self.inner, v as c_uchar);
        }
    }

    pub fn set_lock_timeout(&mut self, lock_timeout: i64) {
        unsafe {
            ffi::rocksdb_transaction_options_set_lock_timeout(self.inner, lock_timeout);
        }
    }

    /// Expiration duration in milliseconds.  If non-negative, transactions that
    /// last longer than this many milliseconds will fail to commit.  If not set,
    /// a forgotten transaction that is never committed, rolled back, or deleted
    /// will never relinquish any locks it holds.  This could prevent keys from
    /// being written by other writers.
    ///
    /// Defaul: `-1`
    ///
    /// # Example
    ///
    /// ```
    /// use exonum_rocksdb::TransactionOptions;
    ///
    /// let mut opts = TransactionOptions::default();
    /// opts.set_expiration(1000);
    /// ```
    pub fn set_expiration(&mut self, expiration: i64) {
        unsafe {
            ffi::rocksdb_transaction_options_set_expiration(self.inner, expiration);
        }
    }

    pub fn set_deadlock_detect_depth(&mut self, depth: i64) {
        unsafe {
            ffi::rocksdb_transaction_options_set_deadlock_detect_depth(self.inner, depth);
        }
    }

    pub fn set_max_write_batch_size(&mut self, size: usize) {
        unsafe {
            ffi::rocksdb_transaction_options_set_max_write_batch_size(self.inner, size);
        }
    }
}

impl Default for TransactionOptions {
    fn default() -> TransactionOptions {
        unsafe {
            let txn_opts = ffi::rocksdb_transaction_options_create();
            if txn_opts.is_null() {
                panic!("couldn't create transaction options");
            }
            Self { inner: txn_opts }
        }
    }
}

impl Drop for TransactionOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transaction_options_destroy(self.inner);
        }
    }
}
