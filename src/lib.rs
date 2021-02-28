//! redis_ts proivdes a small trait with extension functions for the
//! [redis](https://docs.rs/redis) crate to allow
//! working with redis time series data that can be installed as
//! a [redis module](https://oss.redislabs.com/redistimeseries). Time
//! series commands are available as synchronous and asynchronous versions.
//!
//! The crate is called `redis_ts` and you can depend on it via cargo. You will
//! also need redis in your dependencies. It has been tested agains redis 0.20.0
//! but should work with versions higher than that.
//!
//! ```ini
//! [dependencies]
//! redis = "0.20.0"
//! redis_ts = "0.4.0"
//! ```
//!
//! Or via git:
//!
//! ```ini
//! [dependencies.redis_ts]
//! git = "https://github.com/tompro/redis_ts.git"
//! ```
//!
//! With async feature inherited from the [redis](https://docs.rs/redis)
//! crate (either: 'async-std-comp' or 'tokio-comp):
//! ```ini
//! [dependencies]
//! redis = "0.20.0"
//! redis_ts = { version = "0.4.0", features = ['tokio-comp'] }
//! ```
//!
//! # Synchronous usage
//!
//! To enable redis time series commands you simply load the
//! redis_ts::TsCommands into the scope. All redis time series
//! commands will then be available on your redis connection.
//!
//!  
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! use redis::Commands;
//! use redis_ts::{TsCommands, TsOptions};
//!
//! let client = redis::Client::open("redis://127.0.0.1/")?;
//! let mut con = client.get_connection()?;
//!
//! let _:() = con.ts_create("my_ts", TsOptions::default())?;
//! # Ok(()) }
//! ```
//!
//!
//! # Asynchronous usage
//!
//! To enable redis time series async commands you simply load the
//! redis_ts::TsAsyncCommands into the scope. All redis time series
//! commands will then be available on your async redis connection.
//!
//! ```rust,no_run
//! # #[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
//! # async fn run() -> redis::RedisResult<()> {
//! use redis::AsyncCommands;
//! use redis_ts::{AsyncTsCommands, TsOptions};
//!
//! let client = redis::Client::open("redis://127.0.0.1/")?;
//! let mut con = client.get_async_connection().await?;
//!
//! let _:() = con.ts_create("my_ts", TsOptions::default()).await?;
//! # Ok(()) }
//! ```
//!
//! # Supported commands
//!
//! The following examples work with the synchronous and asynchronous
//! API. For simplicity all examples will use the synchronous API. To
//! use them async simply run them whithin an async function and append
//! the .await after the command call.
//!
//! ## TS.CREATE
//! Creates new time series keys. TsOptions can help you build the time
//! series configuration you want to have.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions, TsDuplicatePolicy};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let my_opts = TsOptions::default()
//!   .retention_time(60000)
//!   .uncompressed(false)
//!   .duplicate_policy(TsDuplicatePolicy::Last)
//!   .label("component", "engine")
//!   .label("sensor", "temperature");
//!
//! let _:() = con.ts_create("my_engine", my_opts)?;
//! # Ok(()) }
//! ```
//!
//! ## TS.ALTER
//! Modifies existing time series keys. Note: You can not modify the uncompressed
//! option of an existing time series so the flag will be ignored.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let my_opts = TsOptions::default()
//!   .retention_time(600000)
//!   .label("component", "spark_plug")
//!   .label("sensor", "temperature");
//!
//! let _:() = con.ts_alter("my_engine", my_opts)?;
//! # Ok(()) }
//! ```
//!
//! ## TS.ADD
//! Add a value to time series. When providing time series options with
//! the add command the series will be created if it does not yet exist.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! /// With a timestamp
//! let ts:u64 = con.ts_add("my_engine", 123456789, 36.1)?;
//!
//! /// Auto redis timestamp
//! let now:u64 = con.ts_add_now("my_engine", 36.2)?;
//!
//! /// Add with auto create.
//! let my_opts = TsOptions::default()
//!   .retention_time(600000)
//!   .label("component", "spark_plug")
//!   .label("sensor", "temperature");
//!
//! let create_ts:u64 = con.ts_add_create("my_engine", "*", 35.7, my_opts)?;
//! # Ok(()) }
//! ```
//!
//! ## TS.MADD
//! Add multiple values to one or multiple time series.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let r:Vec<u64> = con.ts_madd(&[
//!   ("my_engine", 1234, 36.0),
//!   ("other_engine", 4321, 33.9)
//! ])?;
//! # Ok(()) }
//! ```
//!
//! ## TS.INCRBY
//! Increment a time series value.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! /// With a timestamp
//! let ts:u64 = con.ts_incrby("my_engine", 123456789, 2)?;
//!
//! /// Auto redis timestamp
//! let now:u64 = con.ts_incrby_now("my_engine", 7.0)?;
//!
//! /// With auto create.
//! let my_opts = TsOptions::default()
//!   .retention_time(600000)
//!   .label("component", "spark_plug")
//!   .label("sensor", "temperature");
//!
//! let create_ts:u64 = con.ts_incrby_create("my_engine", "*", 16.97, my_opts)?;
//! # Ok(()) }
//! ```
//!
//! ## TS.DECRBY
//! Decrement a time series value.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! /// With a timestamp
//! let ts:u64 = con.ts_decrby("my_engine", 123456789, 2)?;
//!
//! /// Auto redis timestamp
//! let now:u64 = con.ts_decrby_now("my_engine", 7.0)?;
//!
//! /// With auto create.
//! let my_opts = TsOptions::default()
//!   .retention_time(600000)
//!   .label("component", "spark_plug")
//!   .label("sensor", "temperature");
//!
//! let create_ts:u64 = con.ts_decrby_create("my_engine", "*", 16.97, my_opts)?;
//! # Ok(()) }
//! ```
//!
//! ## TS.CREATERULE
//! Create time series compaction rules.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsAggregationType};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let _:() = con.ts_createrule("my_engine", "my_engine_avg", TsAggregationType::Avg(5000))?;
//! # Ok(()) }
//! ```
//!
//! ## TS.DELETERULE
//! Delete time series compaction rules.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let _:() = con.ts_deleterule("my_engine", "my_engine_avg")?;
//! # Ok(()) }
//! ```
//!
//! ## TS.RANGE/TS.REVRANGE
//! Query for a range of time series data.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsRange, TsAggregationType};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let first_three_avg:TsRange<u64,f64> = con.ts_range(
//!     "my_engine", "-", "+", Some(3), Some(TsAggregationType::Avg(5000))
//! )?;
//!
//! let range_raw:TsRange<u64,f64> = con.ts_range(
//!     "my_engine", 1234, 5678, None::<usize>, None
//! )?;
//!
//! let rev_range_raw:TsRange<u64,f64> = con.ts_revrange(
//!     "my_engine", 1234, 5678, None::<usize>, None
//! )?;
//! # Ok(()) }
//! ```
//!
//! ## TS.MRANGE/TS.MREVRANGE
//! Batch query multiple ranges of time series data.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsMrange, TsAggregationType, TsFilterOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let first_three_avg:TsMrange<u64,f64> = con.ts_mrange(
//!     "-", "+", Some(3), Some(TsAggregationType::Avg(5000)),
//!     TsFilterOptions::default().equals("sensor", "temperature")
//! )?;
//!
//! let range_raw:TsMrange<u64,f64> = con.ts_mrange(
//!     1234, 5678, None::<usize>, None,
//!     TsFilterOptions::default().equals("sensor", "temperature")
//! )?;
//!
//! let rev_range_raw:TsMrange<u64,f64> = con.ts_mrevrange(
//!     1234, 5678, None::<usize>, None,
//!     TsFilterOptions::default().equals("sensor", "temperature")
//! )?;
//! # Ok(()) }
//! ```
//!
//! ## TS.GET
//! Get the most recent value of a time series.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let latest:Option<(u64,f64)> = con.ts_get("my_engine")?;
//! # Ok(()) }
//! ```
//!
//! ## TS.MGET
//! Get the most recent value of multiple time series.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsMget, TsFilterOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let temperature:TsMget<u64,f64> = con.ts_mget(
//!     TsFilterOptions::default().equals("sensor", "temperature").with_labels(true)
//! )?;
//! # Ok(()) }
//! ```
//!
//! ## TS.INFO
//! Get information about a time series key.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands,TsInfo};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let info:TsInfo = con.ts_info("my_engine")?;
//! # Ok(()) }
//! ```
//!
//! ## TS.QUERYINDEX
//! Get the keys of time series filtered by given filter.
//!
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands,TsFilterOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let index:Vec<String> = con.ts_queryindex(
//!     TsFilterOptions::default().equals("sensor", "temperature")
//! )?;
//! # Ok(()) }
//! ```
//!
#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
pub use crate::async_commands::AsyncTsCommands;

pub use crate::commands::TsCommands;

pub use crate::types::{
    TsAggregationType, TsDuplicatePolicy, TsFilterOptions, TsInfo, TsMget, TsMrange, TsOptions,
    TsRange,
};

#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
mod async_commands;

mod commands;
mod types;
