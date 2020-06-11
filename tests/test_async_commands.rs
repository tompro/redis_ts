extern crate redis;
extern crate redis_ts;
extern crate async_std;

use redis::aio::Connection;
use redis_ts::AsyncTsCommands;
use redis_ts::{TsInfo, TsOptions};
use async_std::task;
use redis::AsyncCommands;
use std::time::{SystemTime, UNIX_EPOCH};

async fn get_con() -> Connection {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    client.get_async_connection().await.unwrap()
}

async fn prepare_ts(name: &str) -> Connection {
	let mut con = get_con().await;
	let _:() = con.del(name).await.unwrap();
	let _:() = con.ts_create(name, TsOptions::default()).await.unwrap();
	con
}

#[test]
fn test_ts_create_info() {
	let res:TsInfo = task::block_on(async {
		let mut con = get_con().await;
		let _:() = con.del("async_test_ts_info").await.unwrap();
		let _:() = con.ts_create(
			"async_test_ts_info", TsOptions::default().label("l", "async_test_ts_info"
		)).await.unwrap();
		let r:TsInfo = con.ts_info("async_test_ts_info").await.unwrap();
		r
	});
	assert_eq!(res.labels, vec![("l".to_string(), "async_test_ts_info".to_string())]);
}

#[test]
fn test_ts_add() {
	let ts:u64 = task::block_on(async {
		let mut con = prepare_ts("async_test_ts_add").await;
		let r:u64 = con.ts_add("async_test_ts_add", 1234567890, 2.2).await.unwrap();
		r
	});

	assert_eq!(ts, 1234567890);
}

#[test]
fn test_ts_add_now() {
	let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
	let ts:u64 = task::block_on(async {
		let mut con = prepare_ts("async_test_ts_add_now").await;
		let ts:u64 = con.ts_add_now("async_test_ts_add_now", 2.2).await.unwrap();
		ts
	});
	assert!(now <= ts);
}
