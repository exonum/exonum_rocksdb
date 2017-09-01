use exonum_rocksdb::{Options, WriteOptions, IteratorMode};
use exonum_rocksdb::optimistic_txn_db::{OptimisticTransactionDB, OptimisticTransactionOptions};
use tempdir::TempDir;

#[test]
fn test_optimistictransactiondb_creation_and_destroy() {
    let temp_dir = TempDir::new("transaction_db_1").unwrap();
    let path = temp_dir.path();
    let _ = OptimisticTransactionDB::open_default(path);
    assert!(OptimisticTransactionDB::destroy(&Options::default(), path).is_ok());
}

#[test]
fn test_optimistictransactiondb_transaction() {
    let temp_dir = TempDir::new("transaction_db_2").unwrap();
    let path = temp_dir.path();
    let db = OptimisticTransactionDB::open_default(path).unwrap();
    let mut w_opts = WriteOptions::default();
    w_opts.set_sync(true);
    let txn_opts = OptimisticTransactionOptions::default();

    {
        let txn1 = db.transaction_begin(&w_opts, &txn_opts);
        let txn2 = db.transaction_begin(&w_opts, &txn_opts);

        assert!(txn1.put(b"a", b"1").is_ok());
        assert!(txn2.put(b"b", b"1").is_ok());
        assert!(txn1.commit().is_ok());
        assert!(txn2.commit().is_ok());

        assert!(txn1.put(b"c", b"1").is_ok());
        assert!(txn2.put(b"c", b"1").is_ok());
        assert!(txn1.commit().is_ok());
        assert!(txn2.commit().is_err());
    }
    {
        let txn = db.transaction_begin(&w_opts, &txn_opts);
        assert_eq!(txn.iterator().count(), 3);
        assert!(txn.get(b"c").unwrap().is_some());
        assert!(txn.delete(b"c").is_ok());
        assert!(txn.get(b"c").unwrap().is_none());
    }
}

#[test]
fn test_optimistictransactiondb_transaction_cf() {
    let temp_dir = TempDir::new("transaction_db_3").unwrap();
    let path = temp_dir.path();
    let opts = Options::default();
    let w_opts = WriteOptions::default();
    let txn_opts = OptimisticTransactionOptions::default();

    {
        let mut db = OptimisticTransactionDB::open_default(path).unwrap();
        let cf1 = db.create_cf("cf1", &opts).unwrap();
        let cf2 = db.create_cf("cf2", &opts).unwrap();
        let txn1 = db.transaction_begin(&w_opts, &txn_opts);
        let txn2 = db.transaction_begin(&w_opts, &txn_opts);

        assert!(txn1.put_cf(cf1, b"a", b"1").is_ok());
        assert!(txn2.put_cf(cf2, b"a", b"1").is_ok());
        assert!(txn2.put_cf(cf1, b"a", b"1").is_ok());
        assert!(txn1.commit().is_ok());
        assert!(txn2.commit().is_err());
    }

    let cf_names = ["cf1", "cf2"];
    let db = OptimisticTransactionDB::open_cf(&opts, path, &cf_names).unwrap();
    let cf1 = db.cf_handle("cf1");
    let cf2 = db.cf_handle("cf2");
    let txn = db.transaction_begin(&w_opts, &txn_opts);

    assert!(cf1.is_some());
    assert!(cf2.is_some());

    assert_eq!(txn.iterator_cf(cf1.unwrap()).unwrap().count(), 1);
    assert_eq!(txn.iterator_cf(cf2.unwrap()).unwrap().count(), 0);

    assert!(txn.get_cf(cf1.unwrap(), b"a").unwrap().is_some());
    assert!(txn.get_cf(cf2.unwrap(), b"a").unwrap().is_none());

    assert!(txn.put_cf(cf2.unwrap(), b"b", b"3").is_ok());
    assert!(txn.get_cf(cf2.unwrap(), b"b").unwrap().is_some());
    assert!(txn.delete_cf(cf2.unwrap(), b"b").is_ok());
    assert!(txn.get_cf(cf2.unwrap(), b"b").unwrap().is_none());
}

#[test]
fn test_optimistictransactiondb_snapshot() {
    let temp_dir = TempDir::new("transaction_db_4").unwrap();
    let path = temp_dir.path();
    let w_opts = WriteOptions::default();
    let txn_opts = OptimisticTransactionOptions::default();

    let db = OptimisticTransactionDB::open_default(path).unwrap();
    let txn = db.transaction_begin(&w_opts, &txn_opts);
    let snapshot = db.snapshot();

    assert!(txn.put(b"a", b"1").is_ok());
    assert!(snapshot.get(b"a").unwrap().is_none());
    assert!(txn.commit().is_ok());
    assert!(snapshot.get(b"a").unwrap().is_none());
    assert_eq!(snapshot.iterator(IteratorMode::Start).count(), 0);

    let snapshot = db.snapshot();
    assert!(snapshot.get(b"a").unwrap().is_some());
    assert_eq!(snapshot.iterator(IteratorMode::Start).count(), 1);
}

#[test]
fn test_optimistictransactiondb_snapshot_cf() {
    let temp_dir = TempDir::new("transaction_db_4").unwrap();
    let path = temp_dir.path();
    let w_opts = WriteOptions::default();
    let txn_opts = OptimisticTransactionOptions::default();

    let mut db = OptimisticTransactionDB::open_default(path).unwrap();
    let cf1 = db.create_cf("cf1", &Options::default()).unwrap();
    let txn = db.transaction_begin(&w_opts, &txn_opts);
    let snapshot = db.snapshot();

    assert!(txn.put_cf(cf1, b"a", b"1").is_ok());
    assert!(snapshot.get_cf(cf1, b"a").unwrap().is_none());
    assert!(txn.commit().is_ok());
    assert!(snapshot.get_cf(cf1, b"a").unwrap().is_none());
    assert_eq!(snapshot.iterator_cf(cf1, IteratorMode::Start).unwrap().count(), 0);

    let snapshot = db.snapshot();
    assert!(snapshot.get_cf(cf1, b"a").unwrap().is_some());
    assert_eq!(snapshot.iterator_cf(cf1, IteratorMode::Start).unwrap().count(), 1);
}
