extern crate async_std;
use crate::async_command_tests::*;
use async_std::task;
use futures::Future;
mod async_command_tests;

fn block_on<F>(f: F) -> F::Output
where
    F: Future,
{
    task::block_on(f)
}

#[test]
fn testing_ts_create_info() {
    let res = block_on(ts_create_info("test_ts_create_info_async_std"));
    verify_ts_create_info(res, "test_ts_create_info_async_std");
}

#[test]
fn test_ts_add() {
    let res = block_on(ts_add("test_ts_add_async_std"));
    verify_ts_add(res);
}

#[test]
fn test_ts_add_now() {
    let res = block_on(ts_add_now("test_ts_add_now_async_std"));
    verify_ts_add_now(res);
}

#[test]
fn test_ts_add_create() {
    let _: () = block_on(ts_add_create("test_ts_add_create_async_std"));
}

#[test]
fn test_ts_add_replace() {
    let _: () = block_on(ts_add_replace("test_ts_add_replace_async_std"));
}

#[test]
fn test_ts_madd() {
    let _: () = block_on(ts_madd("test_ts_madd_async_std"));
}

#[test]
fn test_ts_incrby_now() {
    let _: () = block_on(ts_incrby_now("async_test_ts_incrby_now_std"));
}

#[test]
fn test_ts_decrby_now() {
    let _: () = block_on(ts_decrby_now("async_test_ts_decrby_now_std"));
}

#[test]
fn test_ts_incrby() {
    let _: () = block_on(ts_incrby("async_test_ts_incrby_std"));
}

#[test]
fn test_ts_decrby() {
    let _: () = block_on(ts_decrby("async_test_ts_decrby_std"));
}

#[test]
fn test_ts_incrby_create() {
    let _: () = block_on(ts_incrby_create("async_test_ts_incrby_create_std"));
}

#[test]
fn test_ts_decrby_create() {
    let _: () = block_on(ts_decrby_create("async_test_ts_decrby_create_std"));
}

#[test]
fn test_ts_create_delete_rule() {
    let _: () = block_on(ts_create_delete_rule(
        "async_test_ts_create_delete_rule_std",
    ));
}

#[test]
fn test_ts_get() {
    let _: () = block_on(ts_get("async_test_ts_get_std"));
}

#[test]
fn test_ts_mget() {
    let _: () = block_on(ts_mget("async_test_ts_mget_std"));
}

#[test]
fn test_ts_get_ts_info() {
    let _: () = block_on(ts_get_ts_info("async_test_ts_get_ts_info_std"));
}

#[test]
fn test_ts_range() {
    let _: () = block_on(ts_range("async_test_ts_range_std"));
}

#[test]
fn test_ts_revrange() {
    let _: () = block_on(ts_revrange("async_test_ts_revrange_std"));
}

#[test]
fn test_ts_mrange() {
    let _: () = block_on(ts_mrange("async_test_ts_mrange_std"));
}

#[test]
fn test_ts_mrevrange() {
    let _: () = block_on(ts_mrevrange("async_test_ts_mrevrange_std"));
}

#[test]
fn test_ts_queryindex() {
    let _: () = block_on(ts_queryindex("async_test_ts_queryindex_std"));
}
