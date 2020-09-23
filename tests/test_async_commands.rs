extern crate async_std;
extern crate redis;
extern crate redis_ts;

use async_std::task;
use redis::aio::Connection;
use redis::AsyncCommands;
use redis_ts::AsyncTsCommands;
use redis_ts::{
    TsAggregationType, TsDuplicatePolicy, TsFilterOptions, TsInfo, TsMget, TsMrange, TsOptions,
    TsRange,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

async fn get_con() -> Connection {
    let client = redis::Client::open("redis://localhost/").unwrap();
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

#[test]
fn test_ts_create_info() {
    let res: TsInfo = task::block_on(async {
        let mut con = get_con().await;
        let _: () = con.del("async_test_ts_info").await.unwrap();
        let _: () = con
            .ts_create(
                "async_test_ts_info",
                TsOptions::default()
                    .label("l", "async_test_ts_info")
                    .duplicate_policy(TsDuplicatePolicy::Max),
            )
            .await
            .unwrap();
        let r: TsInfo = con.ts_info("async_test_ts_info").await.unwrap();
        r
    });
    assert_eq!(
        res.labels,
        vec![("l".to_string(), "async_test_ts_info".to_string())]
    );
    assert_eq!(res.duplicate_policy, Some(TsDuplicatePolicy::Max));
}

#[test]
fn test_ts_add() {
    let ts: u64 = task::block_on(async {
        let mut con = prepare_ts("async_test_ts_add").await;
        let r: u64 = con
            .ts_add("async_test_ts_add", 1234567890, 2.2)
            .await
            .unwrap();
        r
    });

    assert_eq!(ts, 1234567890);
}

#[test]
fn test_ts_add_now() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let ts: u64 = task::block_on(async {
        let mut con = prepare_ts("async_test_ts_add_now").await;
        let ts: u64 = con.ts_add_now("async_test_ts_add_now", 2.2).await.unwrap();
        ts
    });
    assert!(now <= ts);
}

#[test]
fn test_ts_add_create() {
    let _: () = task::block_on(async {
        let mut con = get_con().await;
        let _: () = con.del("async_test_ts_add_create").await.unwrap();
        let ts: u64 = con
            .ts_add_create(
                "async_test_ts_add_create",
                1234567890,
                2.2,
                TsOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(ts, 1234567890);

        let ts2: u64 = con
            .ts_add_create("async_test_ts_add_create", "*", 2.3, TsOptions::default())
            .await
            .unwrap();
        assert!(ts2 > ts);
    });
}

#[test]
fn test_ts_add_replace() {
    let _: () = task::block_on(async {
        let mut con = get_con().await;
        let _: () = con.del("async_test_ts_add_replace").await.unwrap();
        let _: () = con
            .ts_create(
                "async_test_ts_add_replace",
                TsOptions::default().duplicate_policy(TsDuplicatePolicy::Last),
            )
            .await
            .unwrap();
        let _: u64 = con
            .ts_add("async_test_ts_add_replace", 1234567890u64, 2.2f64)
            .await
            .unwrap();
        let _: u64 = con
            .ts_add("async_test_ts_add_replace", 1234567890u64, 3.2f64)
            .await
            .unwrap();
        let stored: (u64, f64) = con
            .ts_get("async_test_ts_add_replace")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.1, 3.2);
    });
}

#[test]
fn test_ts_madd() {
    let _: () = task::block_on(async {
        let mut con = prepare_ts("async_test_ts_madd").await;
        let _ = prepare_ts("async_test_ts_madd2").await;
        let expected: Vec<u64> = vec![1234, 4321];
        let res: Vec<u64> = con
            .ts_madd(&[
                ("async_test_ts_madd", 1234, 1.0),
                ("async_test_ts_madd2", 4321, 2.0),
            ])
            .await
            .unwrap();
        assert_eq!(expected, res);
    });
}

#[test]
fn test_ts_incrby_now() {
    let _: () = task::block_on(async {
        let mut con = prepare_ts("async_test_ts_incrby_now").await;
        let _: () = con
            .ts_incrby_now("async_test_ts_incrby_now", 1)
            .await
            .unwrap();
        let v1: Option<(u64, f64)> = con.ts_get("async_test_ts_incrby_now").await.unwrap();
        assert_eq!(v1.unwrap().1, 1.0);
        sleep(1);
        let _: () = con
            .ts_incrby_now("async_test_ts_incrby_now", 5)
            .await
            .unwrap();
        let v2: Option<(u64, f64)> = con.ts_get("async_test_ts_incrby_now").await.unwrap();
        assert_eq!(v2.unwrap().1, 6.0);
    });
}

#[test]
fn test_ts_decrby_now() {
    let _: () = task::block_on(async {
        let mut con = prepare_ts("async_test_ts_decrby_now").await;
        let _: () = con
            .ts_add_now("async_test_ts_decrby_now", 10)
            .await
            .unwrap();
        let v1: Option<(u64, f64)> = con.ts_get("async_test_ts_decrby_now").await.unwrap();
        assert_eq!(v1.unwrap().1, 10.0);
        sleep(1);

        let _: () = con
            .ts_decrby_now("async_test_ts_decrby_now", 1)
            .await
            .unwrap();
        let v1: Option<(u64, f64)> = con.ts_get("async_test_ts_decrby_now").await.unwrap();
        assert_eq!(v1.unwrap().1, 9.0);
        sleep(1);

        let _: () = con
            .ts_decrby_now("async_test_ts_decrby_now", 5)
            .await
            .unwrap();
        let v2: Option<(u64, f64)> = con.ts_get("async_test_ts_decrby_now").await.unwrap();
        assert_eq!(v2.unwrap().1, 4.0);
    });
}

#[test]
fn test_ts_incrby() {
    let _: () = task::block_on(async {
        let mut con = prepare_ts("async_test_ts_incrby").await;

        let _: () = con.ts_incrby("async_test_ts_incrby", 123, 1).await.unwrap();
        let v1: Option<(u64, f64)> = con.ts_get("async_test_ts_incrby").await.unwrap();
        assert_eq!(v1.unwrap(), (123, 1.0));

        let _: () = con
            .ts_incrby("async_test_ts_incrby", 1234, 5)
            .await
            .unwrap();
        let v2: Option<(u64, f64)> = con.ts_get("async_test_ts_incrby").await.unwrap();
        assert_eq!(v2.unwrap(), (1234, 6.0));
    });
}

#[test]
fn test_ts_decrby() {
    let _: () = task::block_on(async {
        let mut con = prepare_ts("async_test_ts_decrby").await;
        let _: () = con.ts_add("async_test_ts_decrby", 12, 10).await.unwrap();
        let v1: Option<(u64, f64)> = con.ts_get("async_test_ts_decrby").await.unwrap();
        assert_eq!(v1.unwrap(), (12, 10.0));

        let _: () = con.ts_decrby("async_test_ts_decrby", 123, 1).await.unwrap();
        let v1: Option<(u64, f64)> = con.ts_get("async_test_ts_decrby").await.unwrap();
        assert_eq!(v1.unwrap(), (123, 9.0));

        let _: () = con
            .ts_decrby("async_test_ts_decrby", 1234, 5)
            .await
            .unwrap();
        let v2: Option<(u64, f64)> = con.ts_get("async_test_ts_decrby").await.unwrap();
        assert_eq!(v2.unwrap(), (1234, 4.0));
    });
}

#[test]
fn test_ts_incrby_create() {
    let _: () = task::block_on(async {
        let mut con = get_con().await;
        let _: () = con.del("async_test_ts_incrby_create").await.unwrap();

        let _: () = con
            .ts_incrby_create("async_test_ts_incrby_create", 123, 1, TsOptions::default())
            .await
            .unwrap();
        let v1: Option<(u64, f64)> = con.ts_get("async_test_ts_incrby_create").await.unwrap();
        assert_eq!(v1.unwrap(), (123, 1.0));

        let _: () = con
            .ts_incrby_create("async_test_ts_incrby_create", 1234, 5, TsOptions::default())
            .await
            .unwrap();
        let v2: Option<(u64, f64)> = con.ts_get("async_test_ts_incrby_create").await.unwrap();
        assert_eq!(v2.unwrap(), (1234, 6.0));
    });
}

#[test]
fn test_ts_decrby_create() {
    let _: () = task::block_on(async {
        let mut con = get_con().await;
        let _: () = con.del("async_test_ts_decrby_create").await.unwrap();

        let _: () = con
            .ts_decrby_create("async_test_ts_decrby_create", 123, 1, TsOptions::default())
            .await
            .unwrap();
        let v1: Option<(u64, f64)> = con.ts_get("async_test_ts_decrby_create").await.unwrap();
        assert_eq!(v1.unwrap(), (123, -1.0));

        let _: () = con
            .ts_decrby_create("async_test_ts_decrby_create", 1234, 5, TsOptions::default())
            .await
            .unwrap();
        let v2: Option<(u64, f64)> = con.ts_get("async_test_ts_decrby_create").await.unwrap();
        assert_eq!(v2.unwrap(), (1234, -6.0));
    });
}

#[test]
fn test_ts_create_delete_rule() {
    let _: () = task::block_on(async {
        let mut con = prepare_ts("async_test_ts_create_delete_rule").await;
        let _ = prepare_ts("async_test_ts_create_delete_rule2").await;
        let _: () = con
            .ts_createrule(
                "async_test_ts_create_delete_rule",
                "async_test_ts_create_delete_rule2",
                TsAggregationType::Avg(5000),
            )
            .await
            .unwrap();

        let info: TsInfo = con
            .ts_info("async_test_ts_create_delete_rule")
            .await
            .unwrap();
        assert_eq!(
            info.rules,
            vec![(
                "async_test_ts_create_delete_rule2".to_string(),
                5000,
                "AVG".to_string()
            )]
        );

        let _: () = con
            .ts_deleterule(
                "async_test_ts_create_delete_rule",
                "async_test_ts_create_delete_rule2",
            )
            .await
            .unwrap();
        let info: TsInfo = con
            .ts_info("async_test_ts_create_delete_rule")
            .await
            .unwrap();
        assert_eq!(info.rules, vec![]);
    });
}

#[test]
fn test_ts_get() {
    let _: () = task::block_on(async {
        let mut con = prepare_ts("async_test_ts_get").await;
        let _: () = con.ts_add("async_test_ts_get", 1234, 2.0).await.unwrap();
        let res: Option<(u64, f64)> = con.ts_get("async_test_ts_get").await.unwrap();
        assert_eq!(Some((1234, 2.0)), res);
    });
}

#[test]
fn test_ts_mget() {
    let _: () = task::block_on(async {
        let mut con = get_con().await;
        let opts: TsOptions = TsOptions::default().label("l", "async_mget");

        let _: () = con.del("async_test_ts_mget").await.unwrap();
        let _: () = con.del("async_test_ts_mget2").await.unwrap();
        let _: () = con.del("async_test_ts_mget3").await.unwrap();

        let _: () = con
            .ts_create("async_test_ts_mget", opts.clone())
            .await
            .unwrap();
        let _: () = con
            .ts_create("async_test_ts_mget2", opts.clone())
            .await
            .unwrap();
        let _: () = con
            .ts_create("async_test_ts_mget3", opts.clone())
            .await
            .unwrap();

        let _: () = con
            .ts_madd(&[
                ("async_test_ts_mget", 12, 1.0),
                ("async_test_ts_mget", 123, 2.0),
                ("async_test_ts_mget", 1234, 3.0),
                ("async_test_ts_mget2", 21, 1.0),
                ("async_test_ts_mget2", 321, 2.0),
                ("async_test_ts_mget2", 4321, 3.0),
            ])
            .await
            .unwrap();

        let res: TsMget<u64, f64> = con
            .ts_mget(
                TsFilterOptions::default()
                    .equals("l", "async_mget")
                    .with_labels(true),
            )
            .await
            .unwrap();

        assert_eq!(res.values.len(), 3);
        assert_eq!(res.values[0].value, Some((1234, 3.0)));
        assert_eq!(res.values[1].value, Some((4321, 3.0)));
        assert_eq!(res.values[2].value, None);
    });
}

#[test]
fn test_ts_get_ts_info() {
    let _: () = task::block_on(async {
        let mut con = get_con().await;
        let _: () = con.del("async_test_ts_get_ts_info").await.unwrap();
        let _: () = con
            .ts_create(
                "async_test_ts_get_ts_info",
                TsOptions::default()
                    .label("a", "b")
                    .duplicate_policy(TsDuplicatePolicy::Block)
                    .chunk_size(4096 * 2),
            )
            .await
            .unwrap();
        let _: () = con
            .ts_add("async_test_ts_get_ts_info", "1234", 2.0)
            .await
            .unwrap();
        let info: TsInfo = con.ts_info("async_test_ts_get_ts_info").await.unwrap();
        assert_eq!(info.total_samples, 1);
        assert_eq!(info.first_timestamp, 1234);
        assert_eq!(info.last_timestamp, 1234);
        assert_eq!(info.chunk_count, 1);
        assert_eq!(info.duplicate_policy, Some(TsDuplicatePolicy::Block));
        assert_eq!(info.chunk_size, 4096 * 2);
        assert_eq!(info.labels, vec![("a".to_string(), "b".to_string())]);
    });
}

#[test]
fn test_ts_range() {
    let _: () = task::block_on(async {
        let mut con = prepare_ts("async_test_ts_range").await;
        let _ = prepare_ts("async_test_ts_range2").await;
        let _: () = con
            .ts_madd(&[
                ("async_test_ts_range", 12, 1.0),
                ("async_test_ts_range", 123, 2.0),
                ("async_test_ts_range", 1234, 3.0),
            ])
            .await
            .unwrap();

        let res: TsRange<u64, f64> = con
            .ts_range("async_test_ts_range", "-", "+", None::<usize>, None)
            .await
            .unwrap();
        assert_eq!(res.values, vec![(12, 1.0), (123, 2.0), (1234, 3.0)]);

        let one_res: TsRange<u64, f64> = con
            .ts_range("async_test_ts_range", "-", "+", Some(1), None)
            .await
            .unwrap();
        assert_eq!(one_res.values, vec![(12, 1.0)]);

        let range_res: TsRange<u64, f64> = con
            .ts_range("async_test_ts_range", 12, 123, None::<usize>, None)
            .await
            .unwrap();
        assert_eq!(range_res.values, vec![(12, 1.0), (123, 2.0)]);

        let sum: TsRange<u64, f64> = con
            .ts_range(
                "async_test_ts_range",
                12,
                123,
                None::<usize>,
                Some(TsAggregationType::Sum(10000)),
            )
            .await
            .unwrap();
        assert_eq!(sum.values, vec![(0, 3.0)]);

        let res: TsRange<u64, f64> = con
            .ts_range("async_test_ts_range2", "-", "+", None::<usize>, None)
            .await
            .unwrap();
        assert_eq!(res.values, vec![]);
    });
}

#[test]
fn test_ts_mrange() {
    let _: () = task::block_on(async {
        let mut con = get_con().await;
        let _: () = con.del("async_test_ts_mrange").await.unwrap();
        let _: () = con.del("async_test_ts_mrange2").await.unwrap();
        let opts: TsOptions = TsOptions::default().label("l", "async_mrange");
        let _: () = con
            .ts_create("async_test_ts_mrange", opts.clone())
            .await
            .unwrap();
        let _: () = con
            .ts_create("async_test_ts_mrange2", opts.clone())
            .await
            .unwrap();
        let _: () = con
            .ts_madd(&[
                ("async_test_ts_mrange", 12, 1.0),
                ("async_test_ts_mrange", 123, 2.0),
                ("async_test_ts_mrange", 1234, 3.0),
                ("async_test_ts_mrange2", 21, 1.0),
                ("async_test_ts_mrange2", 321, 2.0),
                ("async_test_ts_mrange2", 4321, 3.0),
            ])
            .await
            .unwrap();

        let res: TsMrange<u64, f64> = con
            .ts_mrange(
                "-",
                "+",
                None::<usize>,
                None,
                TsFilterOptions::default()
                    .equals("l", "async_mrange")
                    .with_labels(true),
            )
            .await
            .unwrap();
        assert_eq!(res.values.len(), 2);
        assert_eq!(
            res.values[1].values,
            vec![(21, 1.0), (321, 2.0), (4321, 3.0)]
        );
        assert_eq!(res.values[0].key, "async_test_ts_mrange");
        assert_eq!(res.values[1].key, "async_test_ts_mrange2");
        assert_eq!(
            res.values[0].labels,
            vec![("l".to_string(), "async_mrange".to_string())]
        );

        let res2: TsMrange<u64, f64> = con
            .ts_mrange(
                "-",
                "+",
                None::<usize>,
                None,
                TsFilterOptions::default()
                    .equals("none", "existing")
                    .with_labels(true),
            )
            .await
            .unwrap();
        assert!(res2.values.is_empty());
    });
}

#[test]
fn test_ts_queryindex() {
    let _: () = task::block_on(async {
        let mut con = get_con().await;
        let _: () = con.del("async_test_ts_queryindex").await.unwrap();
        let _: () = con
            .ts_create(
                "async_test_ts_queryindex",
                TsOptions::default().label("a", "b"),
            )
            .await
            .unwrap();
        let _: () = con
            .ts_add("async_test_ts_queryindex", "1234", 2.0)
            .await
            .unwrap();
        let index: Vec<String> = con
            .ts_queryindex(TsFilterOptions::default().equals("a", "b"))
            .await
            .unwrap();
        assert!(index.contains(&"async_test_ts_queryindex".to_string()));
    });
}
