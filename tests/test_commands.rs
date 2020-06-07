extern crate redis;
extern crate redis_ts;

use redis::{Connection, Commands, Value};
use redis_ts::{TsCommands, TsOptions, TsInfo, TsAggregationType, TsFilterOptions};

use serial_test::serial;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::thread;

fn get_con() -> Connection {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    client.get_connection().expect("Failed to get connection!")
}

fn default_settings() -> TsOptions {
    TsOptions::default().retention_time(60000).label("a", "b")
}

fn sleep(ms:u64) {
    let millis = Duration::from_millis(ms);
    thread::sleep(millis);
}

#[test]
#[serial]
fn test_create_ts_no_options() {
    let _:() = get_con().del("test_ts").unwrap();
    let r:Value = get_con().ts_create("test_ts", TsOptions::default()).unwrap();
    assert_eq!(Value::Okay, r);
    let info = get_con().ts_info("test_ts").unwrap();
    assert_eq!(info.retention_time, 0);
    assert_eq!(info.labels, vec![]);
}

#[test]
#[serial]
fn test_create_ts_retention() {
    let _:() = get_con().del("test_ts").unwrap();
    let opts = TsOptions::default().retention_time(60000);
    let r:Value = get_con().ts_create("test_ts", opts).unwrap();
    assert_eq!(Value::Okay, r);
    let info:TsInfo = get_con().ts_info("test_ts").unwrap();
    assert_eq!(info.labels, vec![]);
    assert_eq!(info.retention_time, 60000);
}

#[test]
#[serial]
fn test_create_ts_labels() {
    let _:() = get_con().del("test_ts").unwrap();
    let opts = TsOptions::default().label("a", "b");
    let r:Value = get_con().ts_create("test_ts", opts).unwrap();
    assert_eq!(Value::Okay, r);
    let info:TsInfo = get_con().ts_info("test_ts").unwrap();
    assert_eq!(info.labels, vec![("a".to_string(), "b".to_string())]);
    assert_eq!(info.retention_time, 0);
}

#[test]
#[serial]
fn test_create_ts_all() {
    let _:() = get_con().del("test_ts").unwrap();
    let opts = TsOptions::default()
        .retention_time(60000)
        .label("a", "b").label("c", "d")
        .uncompressed(true);
    let r:Value = get_con().ts_create("test_ts", opts).unwrap();
    assert_eq!(Value::Okay, r);
}

#[test]
#[serial]
fn test_ts_add() {
    let _:() = get_con().del("test_ts").unwrap();
    let _:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    let ts:u64 = get_con().ts_add("test_ts", 1234567890, 2.2).unwrap();
    assert_eq!(ts, 1234567890);
}

#[test]
#[serial]
fn test_ts_add_now() {
    let _:() = get_con().del("test_ts").unwrap();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let _:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    let ts:u64 = get_con().ts_add_now("test_ts", 2.2).unwrap();
    assert!(now <= ts);
}

#[test]
#[serial]
fn test_ts_add_create() {
    let _:() = get_con().del("test_ts").unwrap();
    let ts:u64 = get_con().ts_add_create(
        "test_ts", 1234567890, 2.2, default_settings()
    ).unwrap();
    assert_eq!(ts, 1234567890);
    let ts2:u64 = get_con().ts_add_create(
        "test_ts", "*", 2.3, default_settings()
    ).unwrap();
    assert!(ts2 > ts);
}

#[test]
#[serial]
fn test_ts_madd() {
    let _:() = get_con().del("test_ts").unwrap();
    let _:() = get_con().del("test_ts2").unwrap();
    let _:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    let _:Value = get_con().ts_create("test_ts2", default_settings()).unwrap();

    let expected:Vec<u64> = vec![1234, 4321];
    let res:Vec<u64> = get_con().ts_madd(&[("test_ts", 1234, 1.0), ("test_ts2", 4321, 2.0)]).unwrap();
    assert_eq!(expected, res);
}

#[test]
#[serial]
fn test_ts_incrby_now() {
    let _:() = get_con().del("test_ts").unwrap();
    let _:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    
    let _:() = get_con().ts_incrby_now("test_ts", 1).unwrap();
    let v1:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v1.unwrap().1, 1.0);
    sleep(1);
    let _:() = get_con().ts_incrby_now("test_ts", 5).unwrap();
    let v2:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v2.unwrap().1, 6.0);
}

#[test]
#[serial]
fn test_ts_decrby_now() {
    let _:() = get_con().del("test_ts").unwrap();
    let _:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    
    let _:() = get_con().ts_add_now("test_ts", 10).unwrap();
    let v1:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v1.unwrap().1, 10.0);
    sleep(1);

    let _:() = get_con().ts_decrby_now("test_ts", 1).unwrap();
    let v1:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v1.unwrap().1, 9.0);
    sleep(1);

    let _:() = get_con().ts_decrby_now("test_ts", 5).unwrap();
    let v2:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v2.unwrap().1, 4.0);
}

#[test]
#[serial]
fn test_ts_incrby() {
    let _:() = get_con().del("test_ts").unwrap();
    let _:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    
    let _:() = get_con().ts_incrby("test_ts", 123, 1).unwrap();
    let v1:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v1.unwrap(), (123, 1.0));

    let _:() = get_con().ts_incrby("test_ts", 1234, 5).unwrap();
    let v2:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v2.unwrap(), (1234, 6.0));
}

#[test]
#[serial]
fn test_ts_decrby() {
    let _:() = get_con().del("test_ts").unwrap();
    let _:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    let _:() = get_con().ts_add("test_ts", 12, 10).unwrap();
    let v1:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v1.unwrap(), (12, 10.0));

    let _:() = get_con().ts_decrby("test_ts", 123, 1).unwrap();
    let v1:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v1.unwrap(), (123, 9.0));

    let _:() = get_con().ts_decrby("test_ts", 1234, 5).unwrap();
    let v2:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v2.unwrap(), (1234, 4.0));
}

#[test]
#[serial]
fn test_ts_incrby_create() {
    let _:() = get_con().del("test_ts").unwrap();
    
    let _:() = get_con().ts_incrby_create("test_ts", 123, 1, default_settings()).unwrap();
    let v1:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v1.unwrap(), (123, 1.0));

    let _:() = get_con().ts_incrby_create("test_ts", 1234, 5, default_settings()).unwrap();
    let v2:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v2.unwrap(), (1234, 6.0));
}

#[test]
#[serial]
fn test_ts_decrby_create() {
    let _:() = get_con().del("test_ts").unwrap();

    let _:() = get_con().ts_decrby_create("test_ts", 123, 1, default_settings()).unwrap();
    let v1:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v1.unwrap(), (123, -1.0));

    let _:() = get_con().ts_decrby_create("test_ts", 1234, 5, default_settings()).unwrap();
    let v2:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(v2.unwrap(), (1234, -6.0));
}

#[test]
#[serial]
fn test_ts_create_delete_rule() {
    let _:() = get_con().del("test_ts").unwrap();
    let _:() = get_con().del("test_ts2").unwrap();
    let _:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    let _:Value = get_con().ts_create("test_ts2", default_settings()).unwrap();
    let _:() = get_con().ts_createrule(
        "test_ts", "test_ts2", TsAggregationType::Avg(5000)
    ).unwrap();

    let info:TsInfo = get_con().ts_info("test_ts").unwrap();
    assert_eq!(info.rules, vec![("test_ts2".to_string(), 5000, "AVG".to_string())]);

    let _:() = get_con().ts_deleterule("test_ts", "test_ts2").unwrap();
    let info:TsInfo = get_con().ts_info("test_ts").unwrap();
    assert_eq!(info.rules, vec![]);
}

#[test]
#[serial]
fn test_ts_get() {
    let _:() = get_con().del("test_ts").unwrap();
    let _:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    let _:() = get_con().ts_add("test_ts", 1234, 2.0).unwrap();
    let res:Option<(u64,f64)> = get_con().ts_get("test_ts").unwrap();
    assert_eq!(Some((1234, 2.0)), res);
}

#[test]
#[serial]
fn test_ts_get_ts_info() {
    let _:() = get_con().del("test_ts").unwrap();
    let _:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    let _:() = get_con().ts_add("test_ts", "1234", 2.0).unwrap();
    let info:TsInfo = get_con().ts_info("test_ts").unwrap();
    assert_eq!(info.total_samples, 1);
    assert_eq!(info.first_timestamp, 1234);
    assert_eq!(info.last_timestamp, 1234);
    assert_eq!(info.chunk_count, 1);
    assert_eq!(info.labels, vec![("a".to_string(),"b".to_string())]);
}

#[test]
#[serial]
fn test_ts_queryindex() {
    let _:() = get_con().del("test_ts").unwrap();
    let _:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    let _:() = get_con().ts_add("test_ts", "1234", 2.0).unwrap();
    let index = get_con().ts_queryindex(TsFilterOptions::default().equals("a", "b")).unwrap();
    assert!(index.contains(&"test_ts".to_string()));
}