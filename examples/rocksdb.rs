extern crate exonum_rocksdb;

use std::thread;
use std::time::Duration;
use exonum_rocksdb::{TransactionDB, Options, WriteOptions, TransactionOptions, TransactionDBOptions};


fn print_stats(opts: &Options, times: u32) {
    for _ in 0..times {
        println!("\n\n####### DB statistics #########\n\n{}", opts.get_statistics().unwrap());
        thread::sleep(Duration::from_secs(2));
    }
}

fn main() {
    let path = "/tmp/rookkkss";
    
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.enable_statistics();
        opts.set_stats_dump_period_sec(2);

        let mut db_opts = TransactionDBOptions::default();
        db_opts.set_max_num_locks(10);

        let mut txn_opts = TransactionOptions::default();
        txn_opts.set_deadlock_detect(true);
        txn_opts.set_snapshot(true);
        txn_opts.set_expiration(5);

        let mut w_opts = WriteOptions::default();
        w_opts.set_sync(true);

        let db: TransactionDB = match TransactionDB::open(&opts, &db_opts, path) {
            Ok(db) => db,
            Err(e) => panic!("couldn't open database: {}", e),
        };

        let _ = db.put(b"key1", b"value1");
        let _ = db.put(b"key2", b"value2");

        let txn = db.transaction_begin(&w_opts, &txn_opts);
        let _ = txn.put(b"key3", b"value3");

        {
            let txn2 = db.transaction_begin(&w_opts, &txn_opts);
            for (key, value) in txn2.iterator() {
                println!("key: {} value: {}",
                         String::from_utf8(key.to_vec()).unwrap(),
                         String::from_utf8(value.to_vec()).unwrap());
            }

            let _ = txn2.put(b"key4", b"value5");
//            txn2.commit();
        }

        assert!(txn.get(b"key4").unwrap().is_some());

        thread::sleep(Duration::from_millis(1));

        if let Err(e) = txn.commit() {
            panic!("error commiting txn: {}", e);
        }

        let txn = db.transaction_begin(&w_opts, &txn_opts);
        let iter = txn.iterator();

        for (key, value) in iter {
            println!("key: {} value: {}", 
                String::from_utf8(key.to_vec()).unwrap(), 
                String::from_utf8(value.to_vec()).unwrap());
        }

        print_stats(&opts, 1);
    }

    if let Err(e) =  TransactionDB::destroy(&Options::default(), path) {
        println!("Error destroying db: {}", e);
    }
}
