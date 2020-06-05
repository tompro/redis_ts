extern crate redis;
extern crate redis_ts;

use redis::{Connection, Commands, Value};
use redis_ts::{TsCommands, TsOptions, TsInfo};
use serial_test::serial;
use std::time::{SystemTime, UNIX_EPOCH};

fn get_con() -> Connection {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    client.get_connection().expect("Failed to get connection!")
}

fn default_settings() -> TsOptions {
    TsOptions::default().retention_time(60000).label("a", "b")
}

#[test]
#[serial]
fn test_create_ts_no_options() {
    let _:() = get_con().del("test_ts").unwrap();
    let r:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    assert_eq!(Value::Okay, r);
    let info = get_con().ts_info("test_ts").unwrap();
    assert_eq!(info.retention_time, 60000);
    assert_eq!(info.labels, vec![("a".to_string(), "b".to_string())]);
}

#[test]
#[serial]
fn test_create_ts_retention() {
    let _:() = get_con().del("test_ts").unwrap();
    let r:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    assert_eq!(Value::Okay, r);
}

#[test]
#[serial]
fn test_create_ts_labels() {
    let _:() = get_con().del("test_ts").unwrap();
    let r:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
    assert_eq!(Value::Okay, r);
}

#[test]
#[serial]
fn test_create_ts_all() {
    let _:() = get_con().del("test_ts").unwrap();
    let r:Value = get_con().ts_create("test_ts", default_settings()).unwrap();
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
    let ts:u64 = get_con().ts_add_create("test_ts", Some(1234567890), 2.2, default_settings()).unwrap();
    assert_eq!(ts, 1234567890);
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