extern crate async_std;
extern crate redis;
extern crate redis_ts;

use redis::aio::Connection;
use redis::AsyncCommands;
use redis_ts::AsyncTsCommands;
use redis_ts::{
    TsAggregationType, TsDuplicatePolicy, TsFilterOptions, TsInfo, TsMget, TsMrange, TsOptions,
    TsRange, TsRangeQuery,
};
use std::env;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

async fn get_con() -> Connection {
    let client = redis::Client::open(get_redis_url()).unwrap();
    client.get_async_connection().await.unwrap()
}

async fn prepare_ts(name: &str) -> Connection {
    let mut con = get_con().await;
    let _: () = con.del(name).await.unwrap();
    let _: () = con.ts_create(name, TsOptions::default()).await.unwrap();
    con
}

fn sleep(ms: u64) {
    let millis = Duration::from_millis(ms);
    thread::sleep(millis);
}

/// Create and verify ts create info future
pub async fn ts_create_info(name: &str) -> TsInfo {
    let mut con = get_con().await;
    let _: () = con.del(name).await.unwrap();
    let _: () = con
        .ts_create(
            name,
            TsOptions::default()
                .label("l", name)
                .duplicate_policy(TsDuplicatePolicy::Max),
        )
        .await
        .unwrap();
    let r: TsInfo = con.ts_info(name).await.unwrap();
    r
}

pub fn verify_ts_create_info(res: TsInfo, name: &str) {
    assert_eq!(res.labels, vec![("l".to_string(), name.to_string())]);
    assert_eq!(res.duplicate_policy, Some(TsDuplicatePolicy::Max));
}

/// Create/verify ts add
pub async fn ts_add(name: &str) -> u64 {
    let mut con = prepare_ts(name).await;
    let r: u64 = con.ts_add(name, 1234567890, 2.2).await.unwrap();
    r
}

pub fn verify_ts_add(ts: u64) {
    assert_eq!(ts, 1234567890);
}

/// Create/verify ts add now
pub async fn ts_add_now(name: &str) -> (u64, u64) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut con = prepare_ts(name).await;
    let ts: u64 = con.ts_add_now(name, 2.2).await.unwrap();
    (now, ts)
}

pub fn verify_ts_add_now(times: (u64, u64)) {
    assert!(times.0 <= times.1);
}

/// Create/verify ts add create
pub async fn ts_add_create(name: &str) -> () {
    let mut con = get_con().await;
    let _: () = con.del(name).await.unwrap();
    let ts: u64 = con
        .ts_add_create(name, 1234567890, 2.2, TsOptions::default())
        .await
        .unwrap();
    assert_eq!(ts, 1234567890);

    let ts2: u64 = con
        .ts_add_create(name, "*", 2.3, TsOptions::default())
        .await
        .unwrap();
    assert!(ts2 > ts);
}

pub async fn ts_add_replace(name: &str) {
    let mut con = get_con().await;
    let _: () = con.del(name).await.unwrap();
    let _: () = con
        .ts_create(
            name,
            TsOptions::default().duplicate_policy(TsDuplicatePolicy::Last),
        )
        .await
        .unwrap();
    let _: u64 = con.ts_add(name, 1234567890u64, 2.2f64).await.unwrap();
    let _: u64 = con.ts_add(name, 1234567890u64, 3.2f64).await.unwrap();
    let stored: (u64, f64) = con.ts_get(name).await.unwrap().unwrap();
    assert_eq!(stored.1, 3.2);
}

pub async fn ts_madd(name: &str) {
    let second_name = &format!("{:}2", name);
    let mut con = prepare_ts(name).await;
    let _ = prepare_ts(second_name).await;
    let expected: Vec<u64> = vec![1234, 4321];
    let res: Vec<u64> = con
        .ts_madd(&[(name, 1234, 1.0), (second_name, 4321, 2.0)])
        .await
        .unwrap();
    assert_eq!(expected, res);
}

pub async fn ts_incrby_now(name: &str) {
    let mut con = prepare_ts(name).await;
    let _: () = con.ts_incrby_now(name, 1).await.unwrap();
    let v1: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v1.unwrap().1, 1.0);
    sleep(1);
    let _: () = con.ts_incrby_now(name, 5).await.unwrap();
    let v2: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v2.unwrap().1, 6.0);
}

pub async fn ts_decrby_now(name: &str) {
    let mut con = prepare_ts(name).await;
    let _: () = con.ts_add_now(name, 10).await.unwrap();
    let v1: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v1.unwrap().1, 10.0);
    sleep(1);

    let _: () = con.ts_decrby_now(name, 1).await.unwrap();
    let v1: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v1.unwrap().1, 9.0);
    sleep(1);

    let _: () = con.ts_decrby_now(name, 5).await.unwrap();
    let v2: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v2.unwrap().1, 4.0);
}

pub async fn ts_incrby(name: &str) {
    let mut con = prepare_ts(name).await;

    let _: () = con.ts_incrby(name, 123, 1).await.unwrap();
    let v1: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v1.unwrap(), (123, 1.0));

    let _: () = con.ts_incrby(name, 1234, 5).await.unwrap();
    let v2: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v2.unwrap(), (1234, 6.0));
}

pub async fn ts_decrby(name: &str) {
    let mut con = prepare_ts(name).await;
    let _: () = con.ts_add(name, 12, 10).await.unwrap();
    let v1: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v1.unwrap(), (12, 10.0));

    let _: () = con.ts_decrby(name, 123, 1).await.unwrap();
    let v1: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v1.unwrap(), (123, 9.0));

    let _: () = con.ts_decrby(name, 1234, 5).await.unwrap();
    let v2: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v2.unwrap(), (1234, 4.0));
}

pub async fn ts_incrby_create(name: &str) {
    let mut con = get_con().await;
    let _: () = con.del(name).await.unwrap();

    let _: () = con
        .ts_incrby_create(name, 123, 1, TsOptions::default())
        .await
        .unwrap();
    let v1: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v1.unwrap(), (123, 1.0));

    let _: () = con
        .ts_incrby_create(name, 1234, 5, TsOptions::default())
        .await
        .unwrap();
    let v2: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v2.unwrap(), (1234, 6.0));
}

pub async fn ts_decrby_create(name: &str) {
    let mut con = get_con().await;
    let _: () = con.del(name).await.unwrap();

    let _: () = con
        .ts_decrby_create(name, 123, 1, TsOptions::default())
        .await
        .unwrap();
    let v1: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v1.unwrap(), (123, -1.0));

    let _: () = con
        .ts_decrby_create(name, 1234, 5, TsOptions::default())
        .await
        .unwrap();
    let v2: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(v2.unwrap(), (1234, -6.0));
}

pub async fn ts_create_delete_rule(name: &str) {
    let name2 = &format!("{:}2", name);
    let mut con = prepare_ts(name).await;
    let _ = prepare_ts(name2).await;
    let _: () = con
        .ts_createrule(name, name2, TsAggregationType::Avg(5000))
        .await
        .unwrap();

    let info: TsInfo = con.ts_info(name).await.unwrap();
    assert_eq!(
        info.rules,
        vec![(name2.to_string(), 5000, "AVG".to_string())]
    );

    let _: () = con.ts_deleterule(name, name2).await.unwrap();
    let info: TsInfo = con.ts_info(name).await.unwrap();
    assert_eq!(info.rules, vec![]);
}

pub async fn ts_get(name: &str) {
    let mut con = prepare_ts(name).await;
    let _: () = con.ts_add(name, 1234, 2.0).await.unwrap();
    let res: Option<(u64, f64)> = con.ts_get(name).await.unwrap();
    assert_eq!(Some((1234, 2.0)), res);
}

pub async fn ts_mget(name: &str) {
    let name2 = &format!("{:}2", name);
    let name3 = &format!("{:}3", name);
    let label = &format!("{:}label", name);
    let mut con = get_con().await;
    let opts: TsOptions = TsOptions::default().label("l", label);

    let _: () = con.del(name).await.unwrap();
    let _: () = con.del(name2).await.unwrap();
    let _: () = con.del(name3).await.unwrap();

    let _: () = con.ts_create(name, opts.clone()).await.unwrap();
    let _: () = con.ts_create(name2, opts.clone()).await.unwrap();
    let _: () = con.ts_create(name3, opts.clone()).await.unwrap();

    let _: () = con
        .ts_madd(&[
            (name, 12, 1.0),
            (name, 123, 2.0),
            (name, 1234, 3.0),
            (name2, 21, 1.0),
            (name2, 321, 2.0),
            (name2, 4321, 3.0),
        ])
        .await
        .unwrap();

    let res: TsMget<u64, f64> = con
        .ts_mget(
            TsFilterOptions::default()
                .equals("l", label)
                .with_labels(true),
        )
        .await
        .unwrap();

    assert_eq!(res.values.len(), 3);
    assert_eq!(res.values[0].value, Some((1234, 3.0)));
    assert_eq!(res.values[1].value, Some((4321, 3.0)));
    assert_eq!(res.values[2].value, None);
}

pub async fn ts_get_ts_info(name: &str) {
    let mut con = get_con().await;
    let _: () = con.del(name).await.unwrap();
    let _: () = con
        .ts_create(
            name,
            TsOptions::default()
                .label("a", "b")
                .duplicate_policy(TsDuplicatePolicy::Block)
                .chunk_size(4096 * 2),
        )
        .await
        .unwrap();
    let _: () = con.ts_add(name, "1234", 2.0).await.unwrap();
    let info: TsInfo = con.ts_info(name).await.unwrap();
    assert_eq!(info.total_samples, 1);
    assert_eq!(info.first_timestamp, 1234);
    assert_eq!(info.last_timestamp, 1234);
    assert_eq!(info.chunk_count, 1);
    assert_eq!(info.duplicate_policy, Some(TsDuplicatePolicy::Block));
    assert_eq!(info.chunk_size, 4096 * 2);
    assert_eq!(info.labels, vec![("a".to_string(), "b".to_string())]);
}

pub async fn ts_alter(name: &str) {
    let mut con = get_con().await;
    let _: () = con.del(name).await.unwrap();
    let _: () = con
        .ts_create(
            name,
            TsOptions::default()
                .label("a", "b")
                .duplicate_policy(TsDuplicatePolicy::Block)
                .chunk_size(4096 * 2),
        )
        .await
        .unwrap();
    let _: () = con.ts_add(name, "1234", 2.0).await.unwrap();
    let info: TsInfo = con.ts_info(name).await.unwrap();
    assert_eq!(info.chunk_count, 1);
    assert_eq!(info.chunk_size, 4096 * 2);
    assert_eq!(info.labels, vec![("a".to_string(), "b".to_string())]);

    let _: () = con
        .ts_alter(
            name,
            TsOptions::default().chunk_size(4096 * 4).label("c", "d"),
        )
        .await
        .unwrap();
    let info2: TsInfo = con.ts_info(name).await.unwrap();
    assert_eq!(info2.chunk_size, 4096 * 4);
    assert_eq!(info2.labels, vec![("c".to_string(), "d".to_string())]);
}

pub async fn ts_range(name: &str) {
    let name2 = &format!("{:}2", name);
    let mut con = prepare_ts(name).await;
    let _ = prepare_ts(name2).await;
    let _: () = con
        .ts_madd(&[(name, 12, 1.0), (name, 123, 2.0), (name, 1234, 3.0)])
        .await
        .unwrap();

    let query = TsRangeQuery::default();

    let res: TsRange<u64, f64> = con.ts_range(name, query.clone()).await.unwrap();
    assert_eq!(res.values, vec![(12, 1.0), (123, 2.0), (1234, 3.0)]);

    let one_res: TsRange<u64, f64> = con.ts_range(name, query.clone().count(1)).await.unwrap();
    assert_eq!(one_res.values, vec![(12, 1.0)]);

    let range_res: TsRange<u64, f64> = con
        .ts_range(name, query.clone().filter_by_ts(vec![12, 123]))
        .await
        .unwrap();
    assert_eq!(range_res.values, vec![(12, 1.0), (123, 2.0)]);

    let sum: TsRange<u64, f64> = con
        .ts_range(
            name,
            query
                .clone()
                .filter_by_ts(vec![12, 123])
                .aggregation_type(TsAggregationType::Sum(10000)),
        )
        .await
        .unwrap();
    assert_eq!(sum.values, vec![(0, 3.0)]);

    let res: TsRange<u64, f64> = con.ts_range(name2, query.clone()).await.unwrap();
    assert_eq!(res.values, vec![]);
}

pub async fn ts_revrange(name: &str) {
    let name2 = &format!("{:}2", name);
    let mut con = prepare_ts(name).await;
    let _ = prepare_ts(name2).await;
    let _: () = con
        .ts_madd(&[(name, 12, 1.0), (name, 123, 2.0), (name, 1234, 3.0)])
        .await
        .unwrap();

    let query = TsRangeQuery::default();

    let res: TsRange<u64, f64> = con.ts_revrange(name, query.clone()).await.unwrap();
    assert_eq!(res.values, vec![(1234, 3.0), (123, 2.0), (12, 1.0)]);

    let one_res: TsRange<u64, f64> = con.ts_revrange(name, query.clone().count(1)).await.unwrap();
    assert_eq!(one_res.values, vec![(1234, 3.0)]);

    let range_res: TsRange<u64, f64> = con
        .ts_revrange(name, query.clone().filter_by_ts(vec![12, 123]))
        .await
        .unwrap();
    assert_eq!(range_res.values, vec![(123, 2.0), (12, 1.0)]);

    let sum: TsRange<u64, f64> = con
        .ts_revrange(
            name,
            query
                .clone()
                .filter_by_ts(vec![12, 123])
                .aggregation_type(TsAggregationType::Sum(10000)),
        )
        .await
        .unwrap();
    assert_eq!(sum.values, vec![(0, 3.0)]);

    let res: TsRange<u64, f64> = con.ts_revrange(name2, query.clone()).await.unwrap();
    assert_eq!(res.values, vec![]);
}

pub async fn ts_mrange(name: &str) {
    let name2: &str = &format!("{:}2", name);
    let label = &format!("{:}label", name);

    let mut con = get_con().await;
    let _: () = con.del(name).await.unwrap();
    let _: () = con.del(name2).await.unwrap();
    let opts: TsOptions = TsOptions::default().label("l", label);
    let _: () = con.ts_create(name, opts.clone()).await.unwrap();
    let _: () = con.ts_create(name2, opts.clone()).await.unwrap();
    let _: () = con
        .ts_madd(&[
            (name, 12, 1.0),
            (name, 123, 2.0),
            (name, 1234, 3.0),
            (name2, 21, 1.0),
            (name2, 321, 2.0),
            (name2, 4321, 3.0),
        ])
        .await
        .unwrap();

    let query = TsRangeQuery::default();

    let res: TsMrange<u64, f64> = con
        .ts_mrange(
            query.clone(),
            TsFilterOptions::default()
                .equals("l", label)
                .with_labels(true),
        )
        .await
        .unwrap();
    assert_eq!(res.values.len(), 2);
    assert_eq!(
        res.values[1].values,
        vec![(21, 1.0), (321, 2.0), (4321, 3.0)]
    );
    assert_eq!(res.values[0].key, name);
    assert_eq!(res.values[1].key, name2);
    assert_eq!(
        res.values[0].labels,
        vec![("l".to_string(), label.to_string())]
    );

    let res2: TsMrange<u64, f64> = con
        .ts_mrange(
            query.clone(),
            TsFilterOptions::default()
                .equals("none", "existing")
                .with_labels(true),
        )
        .await
        .unwrap();
    assert!(res2.values.is_empty());
}

pub async fn ts_mrevrange(name: &str) {
    let name2: &str = &format!("{:}2", name);
    let label = &format!("{:}label", name);

    let mut con = get_con().await;
    let _: () = con.del(name).await.unwrap();
    let _: () = con.del(name2).await.unwrap();
    let opts: TsOptions = TsOptions::default().label("l", label);
    let _: () = con.ts_create(name, opts.clone()).await.unwrap();
    let _: () = con.ts_create(name2, opts.clone()).await.unwrap();
    let _: () = con
        .ts_madd(&[
            (name, 12, 1.0),
            (name, 123, 2.0),
            (name, 1234, 3.0),
            (name2, 21, 1.0),
            (name2, 321, 2.0),
            (name2, 4321, 3.0),
        ])
        .await
        .unwrap();

    let query = TsRangeQuery::default();

    let res: TsMrange<u64, f64> = con
        .ts_mrevrange(
            query.clone(),
            TsFilterOptions::default()
                .equals("l", label)
                .with_labels(true),
        )
        .await
        .unwrap();
    assert_eq!(res.values.len(), 2);
    assert_eq!(
        res.values[1].values,
        vec![(4321, 3.0), (321, 2.0), (21, 1.0)]
    );
    assert_eq!(res.values[0].key, name);
    assert_eq!(res.values[1].key, name2);
    assert_eq!(
        res.values[0].labels,
        vec![("l".to_string(), label.to_string())]
    );

    let res2: TsMrange<u64, f64> = con
        .ts_mrevrange(
            query.clone(),
            TsFilterOptions::default()
                .equals("none", "existing")
                .with_labels(true),
        )
        .await
        .unwrap();
    assert!(res2.values.is_empty());
}

pub async fn ts_queryindex(name: &str) {
    let mut con = get_con().await;
    let _: () = con.del(name).await.unwrap();
    let _: () = con
        .ts_create(name, TsOptions::default().label("a", "b"))
        .await
        .unwrap();
    let _: () = con.ts_add(name, "1234", 2.0).await.unwrap();
    let index: Vec<String> = con
        .ts_queryindex(TsFilterOptions::default().equals("a", "b"))
        .await
        .unwrap();
    assert!(index.contains(&name.to_string()));
}

fn get_redis_url() -> String {
    let redis_host_key = "REDIS_HOST";
    let redis_host_port = "REDIS_PORT";

    let redis_host = match env::var(redis_host_key) {
        Ok(host) => host,
        _ => "localhost".to_string(),
    };

    let redis_port = match env::var(redis_host_port) {
        Ok(port) => port,
        _ => "6379".to_string(),
    };

    format!("redis://{}:{}/", redis_host, redis_port)
}
