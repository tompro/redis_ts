extern crate tokio;
use crate::async_command_tests::*;
use futures::Future;
mod async_command_tests;

fn block_on<F>(f: F) -> F::Output
where
    F: Future,
{
    let mut builder = tokio::runtime::Builder::new_current_thread();
    let runtime = builder.enable_io().build().unwrap();
    runtime.block_on(f)
}

#[test]
fn test_ts_create_info() {
    let res = block_on(ts_create_info("test_ts_create_info_async_tokio"));
    verify_ts_create_info(res, "test_ts_create_info_async_tokio");
}

#[test]
fn test_ts_add() {
    let res = block_on(ts_add("test_ts_add_async_tokio"));
    verify_ts_add(res);
}

#[test]
fn test_ts_add_now() {
    let res = block_on(ts_add_now("test_ts_add_now_async_tokio"));
    verify_ts_add_now(res);
}

#[test]
fn test_ts_add_create() {
    let _: () = block_on(ts_add_create("test_ts_add_create_async_tokio"));
}

#[test]
fn test_ts_add_replace() {
    let _: () = block_on(ts_add_replace("test_ts_add_replace_async_tokio"));
}

#[test]
fn test_ts_madd() {
    let _: () = block_on(ts_madd("test_ts_madd_async_tokio"));
}

#[test]
fn test_ts_incrby_now() {
    let _: () = block_on(ts_incrby_now("async_test_ts_incrby_now_tokio"));
}

#[test]
fn test_ts_decrby_now() {
    let _: () = block_on(ts_decrby_now("async_test_ts_decrby_now_tokio"));
}

#[test]
fn test_ts_incrby() {
    let _: () = block_on(ts_incrby("async_test_ts_incrby_tokio"));
}

#[test]
fn test_ts_decrby() {
    let _: () = block_on(ts_decrby("async_test_ts_decrby_tokio"));
}

#[test]
fn test_ts_incrby_create() {
    let _: () = block_on(ts_incrby_create("async_test_ts_incrby_create_tokio"));
}

#[test]
fn test_ts_decrby_create() {
    let _: () = block_on(ts_decrby_create("async_test_ts_decrby_create_tokio"));
}

#[test]
fn test_ts_create_delete_rule() {
    let _: () = block_on(ts_create_delete_rule(
        "async_test_ts_create_delete_rule_tokio",
    ));
}

#[test]
fn test_ts_get() {
    let _: () = block_on(ts_get("async_test_ts_get_tokio"));
}

#[test]
fn test_ts_mget() {
    let _: () = block_on(ts_mget("async_test_ts_mget_tokio"));
}

#[test]
fn test_ts_get_ts_info() {
    let _: () = block_on(ts_get_ts_info("async_test_ts_get_ts_info_tokio"));
}

#[test]
fn test_ts_alter() {
    let _: () = block_on(ts_alter("async_test_ts_alter_tokio"));
}

#[test]
fn test_ts_range() {
    let _: () = block_on(ts_range("async_test_ts_range_tokio"));
}

#[test]
fn test_ts_revrange() {
    let _: () = block_on(ts_revrange("async_test_ts_revrange_tokio"));
}

#[test]
fn test_ts_mrange() {
    let _: () = block_on(ts_mrange("async_test_ts_mrange_tokio"));
}

#[test]
fn test_ts_mrevrange() {
    let _: () = block_on(ts_mrevrange("async_test_ts_mrevrange_tokio"));
}

#[test]
fn test_ts_queryindex() {
    let _: () = block_on(ts_queryindex("async_test_ts_queryindex_tokio"));
}
